package clone

import (
	"context"
	"database/sql"
	"github.com/pkg/errors"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

var (
	tablesTotalMetric = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tables",
			Help: "How many total tables to do.",
		},
	)
	rowCountMetric = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "estimated_rows",
			Help: "How many total tables to do.",
		},
		[]string{"table"},
	)
	tablesDoneMetric = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "tables_done",
			Help: "How many tables done.",
		},
	)
)

func init() {
	prometheus.MustRegister(tablesTotalMetric)
	prometheus.MustRegister(rowCountMetric)
	prometheus.MustRegister(tablesDoneMetric)
}

//// processTable reads/diffs and issues writes for a table (it's increasingly inaccurately named)
//func processTable(ctx context.Context, source DBReader, target DBReader, table *Table, config WriterConfig, writer *sql.DB, writerLimiter core.Limiter) error {
//	logger := logrus.WithField("table", table.Name)
//	start := time.Now()
//	logger.WithTime(start).Infof("start %v", table.Name)
//
//	var chunkingDuration time.Duration
//
//	updates := 0
//	deletes := 0
//	inserts := 0
//	chunkCount := 0
//
//	g, ctx := errgroup.WithContext(ctx)
//
//	// Chunk up the table
//	chunks := make(chan Chunk)
//	g.Go(func() error {
//		err := generateTableChunks(ctx, config.ReaderConfig, source, table, chunks)
//		chunkingDuration = time.Since(start)
//		close(chunks)
//		if err != nil {
//			return errors.WithStack(err)
//		}
//		return nil
//	})
//
//	// Diff each chunk as they are produced
//	diffs := make(chan Diff)
//	g.Go(func() error {
//		readerParallelism := semaphore.NewWeighted(config.ReaderParallelism)
//		g, ctx := errgroup.WithContext(ctx)
//		for c := range chunks {
//			chunk := c
//			err := readerParallelism.Acquire(ctx, 1)
//			if err != nil {
//				return errors.WithStack(err)
//			}
//			g.Go(func() (err error) {
//				defer readerParallelism.Release(1)
//				if config.NoDiff {
//					err = readChunk(ctx, config.ReaderConfig, source, chunk, diffs)
//				} else {
//					err = diffChunk(ctx, config.ReaderConfig, source, target, chunk, diffs)
//				}
//				return errors.WithStack(err)
//			})
//			chunkCount++
//		}
//		err := g.Wait()
//		if err != nil {
//			return errors.WithStack(err)
//		}
//
//		// All diffing done, close the diffs channel
//		close(diffs)
//		return nil
//	})
//
//	// Batch up the diffs
//	batches := make(chan Batch)
//	g.Go(func() error {
//		err := BatchTableWrites(ctx, diffs, batches)
//		close(batches)
//		if err != nil {
//			return errors.WithStack(err)
//		}
//		return nil
//	})
//
//	// Write every batch
//	g.Go(func() error {
//		writerParallelism := semaphore.NewWeighted(config.ReaderParallelism)
//		g, ctx := errgroup.WithContext(ctx)
//		for batch := range batches {
//			size := len(batch.Rows)
//			switch batch.Type {
//			case Update:
//				updates += size
//			case Delete:
//				deletes += size
//			case Insert:
//				inserts += size
//			}
//			err := scheduleWriteBatch(ctx, config, writerParallelism, writerLimiter, g, writer, batch)
//			if err != nil {
//				return errors.WithStack(err)
//			}
//		}
//		err := g.Wait()
//		if err != nil {
//			return errors.WithStack(err)
//		}
//		return nil
//	})
//
//	err := g.Wait()
//
//	elapsed := time.Since(start)
//
//	logger = logger.
//		WithField("duration", elapsed).
//		WithField("chunking", chunkingDuration).
//		WithField("chunks", chunkCount).
//		WithField("inserts", inserts).
//		WithField("deletes", deletes).
//		WithField("updates", updates)
//
//	if err != nil {
//		return errors.WithStack(err)
//	}
//
//	logger.Infof("success %v", table.Name)
//
//	return nil
//}

type Reader struct {
	config ReaderConfig
	table  *Table
	source *sql.DB
	target *sql.DB

	sourceRetry RetryOptions
	targetRetry RetryOptions
}

func (r *Reader) Diff(ctx context.Context, diffs chan Diff) error {
	return r.read(ctx, diffs, true)
}

func (r *Reader) Read(ctx context.Context, diffs chan Diff) error {
	// TODO this can be refactored to a separate method
	return r.read(ctx, diffs, false)
}

func (r *Reader) read(ctx context.Context, diffsCh chan Diff, diff bool) error {
	// Generate chunks of source table
	chunks, err := generateTableChunks2(ctx, r.table, r.source, r.sourceRetry)
	if err != nil {
		return errors.WithStack(err)
	}

	// Generate diffs from all chunks
	readerParallelism := semaphore.NewWeighted(r.config.ReaderParallelism)
	g, ctx := errgroup.WithContext(ctx)
	g.Go(func() error {
		for _, c := range chunks {
			chunk := c
			err := readerParallelism.Acquire(ctx, 1)
			if err != nil {
				return errors.WithStack(err)
			}
			g.Go(func() (err error) {
				defer readerParallelism.Release(1)
				var diffs []Diff
				if diff {
					diffs, err = r.diffChunk(ctx, chunk)
				} else {
					diffs, err = r.readChunk(ctx, chunk)
				}

				if err != nil {
					if r.config.Consistent {
						return errors.WithStack(err)
					}

					log.WithField("table", chunk.Table.Name).
						WithError(err).
						WithContext(ctx).
						Warnf("failed to read chunk %s[%d - %d] after retries and backoff, "+
							"since this is a best effort clone we just give up: %+v",
							chunk.Table.Name, chunk.Start, chunk.End, err)
					return nil
				}

				for _, diff := range diffs {
					select {
					case diffsCh <- diff:
					case <-ctx.Done():
						return ctx.Err()
					}
				}

				return nil
			})
		}
		return nil
	})
	err = g.Wait()
	if err != nil {
		return errors.WithStack(err)
	}

	// All diffing done, close the diffs channel
	close(diffsCh)
	return nil
}

func NewReader(
	config ReaderConfig,
	table *Table,
	source *sql.DB,
	sourceLimiter core.Limiter,
	target *sql.DB,
	targetLimiter core.Limiter,
) *Reader {
	return &Reader{
		config: config,
		table:  table,
		source: source,
		sourceRetry: RetryOptions{
			Limiter:       sourceLimiter,
			AcquireMetric: readLimiterDelay.WithLabelValues("source"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
		target: target,
		targetRetry: RetryOptions{
			Limiter:       targetLimiter,
			AcquireMetric: readLimiterDelay.WithLabelValues("target"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
	}
}
