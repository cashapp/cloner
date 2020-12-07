package clone

import (
	"context"
	"database/sql"
	"time"

	"github.com/pkg/errors"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"vitess.io/vitess/go/vt/proto/topodata"
)

var (
	writesEnqueued = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writes_enqueued",
			Help: "How many writes, partitioned by table and type (insert, update, delete).",
		},
		[]string{"table", "type"},
	)
	writesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writes_processed",
			Help: "How many writes, partitioned by table and type (insert, update, delete).",
		},
		[]string{"table", "type"},
	)
)

func init() {
	prometheus.MustRegister(writesEnqueued)
	prometheus.MustRegister(writesProcessed)
}

// processTable reads/diffs and issues writes for a table (it's increasingly inaccurately named)
func processTable(ctx context.Context, source DBReader, target DBReader, table *Table, cmd *Clone, writer *sql.DB, writerLimiter core.Limiter, readerLimiter *semaphore.Weighted, targetFilter []*topodata.KeyRange) error {
	logger := log.WithField("table", table.Name)
	start := time.Now()
	logger.WithTime(start).Infof("start")

	var chunkingDuration time.Duration

	updates := 0
	deletes := 0
	inserts := 0
	chunkCount := 0

	g, ctx := errgroup.WithContext(ctx)

	// Chunk up the table
	chunks := make(chan Chunk, cmd.QueueSize)
	g.Go(func() error {
		err := readerLimiter.Acquire(ctx, 1)
		if err != nil {
			return errors.WithStack(err)
		}
		defer readerLimiter.Release(1)

		logger := logger.WithField("task", "chunker")
		err = GenerateTableChunks(ctx, source, table, cmd.ChunkSize, cmd.ChunkingTimeout, chunks)
		chunkingDuration = time.Since(start)
		close(chunks)
		if err != nil {
			logger.WithError(err).Errorf("err: %+v\ncontext error: %+v", err, ctx.Err())
			return errors.WithStack(err)
		}
		return nil
	})

	// Diff each chunk as they are produced
	diffs := make(chan Diff)
	g.Go(func() error {
		g, ctx := errgroup.WithContext(ctx)
		for c := range chunks {
			chunk := c
			err := readerLimiter.Acquire(ctx, 1)
			if err != nil {
				return errors.WithStack(err)
			}
			g.Go(func() error {
				defer readerLimiter.Release(1)

				err := diffChunk(ctx, cmd.ReaderConfig, source, target, targetFilter, chunk, diffs)
				return errors.WithStack(err)
			})
			chunkCount++
		}
		err := g.Wait()
		if err != nil {
			logger.WithField("task", "differ").WithError(err).Errorf("err: %+v\ncontext error: %+v", err, ctx.Err())
			return errors.WithStack(err)
		}

		// All diffing done, close the diffs channel
		close(diffs)
		return nil
	})

	// Batch up the diffs
	batches := make(chan Batch, cmd.QueueSize)
	g.Go(func() error {
		err := BatchTableWrites(ctx, cmd.WriteBatchSize, diffs, batches)
		close(batches)
		if err != nil {
			logger.WithField("task", "writer").WithError(err).Errorf("err: %+v\ncontext error: %+v", err, ctx.Err())
			return errors.WithStack(err)
		}
		return nil
	})

	// Write every batch
	g.Go(func() error {
		g, ctx := errgroup.WithContext(ctx)
		for batch := range batches {
			size := len(batch.Rows)
			switch batch.Type {
			case Update:
				updates += size
			case Delete:
				deletes += size
			case Insert:
				inserts += size
			}
			writesEnqueued.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(len(batch.Rows)))
			scheduleWriteBatch(ctx, cmd, writerLimiter, g, writer, batch)
		}
		err := g.Wait()
		if err != nil {
			logger.WithField("task", "writer").WithError(err).Errorf("err: %+v\ncontext error: %+v", err, ctx.Err())
			return errors.WithStack(err)
		}
		return nil
	})

	err := g.Wait()

	elapsed := time.Since(start)

	logger = logger.
		WithField("duration", elapsed).
		WithField("chunking", chunkingDuration).
		WithField("chunks", chunkCount).
		WithField("inserts", inserts).
		WithField("deletes", deletes).
		WithField("updates", updates)

	if err != nil {
		logger.WithError(err).Errorf("err: %+v\ncontext error: %+v", err, ctx.Err())
		return errors.WithStack(err)
	}

	logger.Infof("success")

	return nil
}
