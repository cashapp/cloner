package clone

import (
	"context"
	"database/sql"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/pkg/errors"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"time"
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

// processTable reads/diffs and issues writes for a table (it's increasingly inaccurately named)
func processTable(ctx context.Context, source DBReader, target DBReader, table *Table, cmd *Clone, writer *sql.DB, writerLimiter core.Limiter) error {
	logger := logrus.WithField("table", table.Name)
	start := time.Now()
	logger.WithTime(start).Infof("start %v", table.Name)

	var chunkingDuration time.Duration

	updates := 0
	deletes := 0
	inserts := 0
	chunkCount := 0

	g, ctx := errgroup.WithContext(ctx)

	// Chunk up the table
	chunks := make(chan Chunk)
	g.Go(func() error {
		err := GenerateTableChunks(ctx, cmd.ReaderConfig, source, table, chunks)
		chunkingDuration = time.Since(start)
		close(chunks)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	// Diff each chunk as they are produced
	diffs := make(chan Diff)
	g.Go(func() error {
		readerParallelism := semaphore.NewWeighted(cmd.ReaderParallelism)
		g, ctx := errgroup.WithContext(ctx)
		for c := range chunks {
			chunk := c
			err := readerParallelism.Acquire(ctx, 1)
			if err != nil {
				return errors.WithStack(err)
			}
			g.Go(func() (err error) {
				defer readerParallelism.Release(1)
				if cmd.NoDiff {
					err = readChunk(ctx, cmd.ReaderConfig, source, chunk, diffs)
				} else {
					err = diffChunk(ctx, cmd.ReaderConfig, source, target, chunk, diffs)
				}
				return errors.WithStack(err)
			})
			chunkCount++
		}
		err := g.Wait()
		if err != nil {
			return errors.WithStack(err)
		}

		// All diffing done, close the diffs channel
		close(diffs)
		return nil
	})

	// Batch up the diffs
	batches := make(chan Batch)
	g.Go(func() error {
		err := BatchTableWrites(ctx, diffs, batches)
		close(batches)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	// Write every batch
	g.Go(func() error {
		writerParallelism := semaphore.NewWeighted(cmd.ReaderParallelism)
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
			err := scheduleWriteBatch(ctx, cmd, writerParallelism, writerLimiter, g, writer, batch)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		err := g.Wait()
		if err != nil {
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
		return errors.WithStack(err)
	}

	logger.Infof("success %v", table.Name)

	return nil
}

type Reader struct {
	config ReaderConfig
}

func (r *Reader) Diff(ctx context.Context, diffs chan Diff) error {
	if r.config.TableParallelism == 0 {
		return errors.Errorf("need more parallelism")
	}

	sourceReader, err := r.config.Source.ReaderDB()
	if err != nil {
		return errors.WithStack(err)
	}
	// Refresh connections regularly so they don't go stale
	sourceReader.SetConnMaxLifetime(time.Minute)
	sourceReader.SetMaxOpenConns(r.config.ReaderCount)
	sourceReaderCollector := sqlstats.NewStatsCollector("source_reader", sourceReader)
	prometheus.MustRegister(sourceReaderCollector)
	defer prometheus.Unregister(sourceReaderCollector)
	limitedSourceReader := Limit(
		sourceReader,
		makeLimiter("source_reader_limiter", r.config.ReadTimeout),
		readLimiterDelay.WithLabelValues("source"))

	// Target reader
	// We can use a connection pool of unsynced connections for the target because the assumption is there are no
	// other writers to the target during the clone
	targetReader, err := r.config.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	// Refresh connections regularly so they don't go stale
	targetReader.SetConnMaxLifetime(time.Minute)
	targetReader.SetMaxOpenConns(r.config.ReaderCount)
	targetReaderCollector := sqlstats.NewStatsCollector("target_reader", targetReader)
	prometheus.MustRegister(targetReaderCollector)
	defer prometheus.Unregister(targetReaderCollector)
	limitedTargetReader := Limit(
		targetReader,
		makeLimiter("target_reader_limiter", r.config.ReadTimeout),
		readLimiterDelay.WithLabelValues("target"))

	// Load tables
	// TODO in consistent clone we should diff the schema of the source with the target,
	//      for now we just use the target schema
	tables, err := LoadTables(ctx, r.config)
	if err != nil {
		return errors.WithStack(err)
	}

	var tablesToDo []string
	for _, t := range tables {
		tablesToDo = append(tablesToDo, t.Name)
	}
	logrus.Infof("starting to diff tables: %v", tablesToDo)

	tablesTotalMetric.Add(float64(len(tables)))
	for _, table := range tables {
		rowCountMetric.WithLabelValues(table.Name).Add(float64(table.EstimatedRows))
	}

	chunks := make(chan Chunk)
	g, ctx := errgroup.WithContext(ctx)

	// TODO the parallelism here could be refactored, we should do like we do in processTable, one table at the time

	// Generate chunks of all source tables
	g.Go(func() error {
		var tablesDone []string

		g, ctx := errgroup.WithContext(ctx)
		for _, t := range tables {
			table := t
			g.Go(func() error {
				err := GenerateTableChunks(ctx, r.config, limitedSourceReader, table, chunks)

				tablesDoneMetric.Inc()
				tablesDone = append(tablesDone, t.Name)
				var tablesToDo []string
				for _, t := range tables {
					if !contains(tablesDone, t.Name) {
						tablesToDo = append(tablesToDo, t.Name)
					}
				}

				logrus.Infof("table done: %v", t.Name)
				logrus.Infof("tables done: %v", tablesDone)
				logrus.Infof("tables left to do: %v", tablesToDo)
				return err
			})
		}
		err := g.Wait()
		if err != nil {
			return errors.WithStack(err)
		}
		close(chunks)
		return nil
	})

	// Generate diffs from all chunks
	g.Go(func() error {
		readerParallelism := semaphore.NewWeighted(r.config.ReaderParallelism)
		g, ctx := errgroup.WithContext(ctx)
		for c := range chunks {
			chunk := c
			err := readerParallelism.Acquire(ctx, 1)
			if err != nil {
				return errors.WithStack(err)
			}
			g.Go(func() (err error) {
				defer readerParallelism.Release(1)
				err = diffChunk(ctx, r.config, limitedSourceReader, limitedTargetReader, chunk, diffs)
				return errors.WithStack(err)
			})
		}
		err := g.Wait()
		if err != nil {
			return errors.WithStack(err)
		}

		// All diffing done, close the diffs channel
		close(diffs)
		return nil
	})

	return g.Wait()
}

func NewReader(config ReaderConfig) *Reader {
	return &Reader{config: config}
}
