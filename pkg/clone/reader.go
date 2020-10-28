package clone

import (
	"context"
	"database/sql"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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

// ReadTables generates batches for each table
func ReadTables(ctx context.Context, chunkerConn *sql.Conn, tableCh chan *Table, cmd *Clone, writer *sql.DB, diffRequests chan DiffRequest) error {
	for {
		select {
		case table, more := <-tableCh:
			if !more {
				return nil
			}
			err := readTable(ctx, chunkerConn, table, cmd, writer, diffRequests)
			if err != nil {
				return errors.WithStack(err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// readTables generates write batches for one table
func readTable(ctx context.Context, chunkerConn *sql.Conn, table *Table, cmd *Clone, writer *sql.DB, diffRequests chan DiffRequest) error {
	logger := log.WithField("task", "reader").WithField("table", table.Name)
	start := time.Now()
	logger.WithTime(start).Infof("start")

	var chunkingDuration time.Duration

	chunks := make(chan Chunk, cmd.QueueSize)
	diffs := make(chan Diff, cmd.QueueSize)
	batches := make(chan Batch, cmd.QueueSize)

	updates := 0
	deletes := 0
	inserts := 0
	chunkCount := 0

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		ctx, cancel := context.WithTimeout(ctx, cmd.ReadTimeout)
		defer cancel()

		err := generateTableChunks(ctx, chunkerConn, table, cmd.ChunkSize, chunks)
		chunkingDuration = time.Since(start)
		close(chunks)
		return errors.WithStack(err)
	})
	// Request diffing for every chunk
	g.Go(func() error {
		done := &sync.WaitGroup{}
		for chunk := range chunks {
			done.Add(1)
			select {
			case diffRequests <- DiffRequest{chunk, diffs, done}:
			case <-ctx.Done():
				return nil
			}
			chunkCount++
		}
		// TODO this is a smell that we have to do a context friendly WaitGroup wait here...
		WaitGroupWait(ctx, done)
		// All diffing done, close the diffs channel
		close(diffs)
		return nil
	})
	g.Go(func() error {
		err := BatchTableWrites(ctx, cmd.WriteBatchSize, diffs, batches)
		close(batches)
		return errors.WithStack(err)
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
			scheduleWriteBatch(ctx, cmd, g, writer, batch)
		}
		return g.Wait()
	})

	if err := g.Wait(); err != nil {
		logger.WithError(err).Errorf("%v", err)
		return errors.WithStack(err)
	}

	elapsed := time.Since(start)
	logger.
		WithField("duration", elapsed).
		WithField("chunking", chunkingDuration).
		WithField("chunks", chunkCount).
		WithField("inserts", inserts).
		WithField("deletes", deletes).
		WithField("updates", updates).
		Infof("done")

	return nil
}

// WaitGroupWait is a context friendly wait on a WaitGroup
func WaitGroupWait(ctx context.Context, wg *sync.WaitGroup) {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		close(ch)
	}()
	select {
	case <-ctx.Done():
	case <-ch:
	}
}
