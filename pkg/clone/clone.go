package clone

import (
	"context"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/semaphore"
	_ "net/http/pprof"
	"time"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Clone struct {
	WriterConfig
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// Run finds any differences between source and target
func (cmd *Clone) Run() error {
	var err error

	start := time.Now()

	err = cmd.ReaderConfig.LoadConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	logrus.Infof("using config: %v", cmd)

	err = cmd.run()

	elapsed := time.Since(start)
	logger := logrus.WithField("duration", elapsed)
	if err != nil {
		if stackErr, ok := err.(stackTracer); ok {
			logger = logger.WithField("stacktrace", stackErr.StackTrace())
		}
		logger.WithError(err).Errorf("error: %+v", err)
	} else {
		logger.Infof("full clone success")
	}

	return errors.WithStack(err)
}

func (cmd *Clone) run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if cmd.TableParallelism == 0 {
		return errors.Errorf("need more parallelism")
	}

	// Load tables
	// TODO in consistent clone we should diff the schema of the source with the target,
	//      for now we just use the target schema
	tables, err := LoadTables(ctx, cmd.ReaderConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	// TODO set up synced connections from the source
	if cmd.Consistent {
		return errors.Errorf("not implemented yet")
	}

	sourceReader, err := cmd.Source.ReaderDB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer sourceReader.Close()
	// Refresh connections regularly so they don't go stale
	sourceReader.SetConnMaxLifetime(time.Minute)
	sourceReader.SetMaxOpenConns(cmd.ReaderCount)
	sourceReaderCollector := sqlstats.NewStatsCollector("source_reader", sourceReader)
	prometheus.MustRegister(sourceReaderCollector)
	defer prometheus.Unregister(sourceReaderCollector)

	// Target reader
	// We can use a connection pool of unsynced connections for the target because the assumption is there are no
	// other writers to the target during the clone
	// TODO we only have to open the target DB if NoDiff is set to false
	targetReader, err := cmd.Target.ReaderDB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer targetReader.Close()
	// Refresh connections regularly so they don't go stale
	targetReader.SetConnMaxLifetime(time.Minute)
	targetReader.SetMaxOpenConns(cmd.ReaderCount)
	targetReaderCollector := sqlstats.NewStatsCollector("target_reader", targetReader)
	prometheus.MustRegister(targetReaderCollector)
	defer prometheus.Unregister(targetReaderCollector)

	writer, err := cmd.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer writer.Close()
	// Refresh connections regularly so they don't go stale
	writer.SetConnMaxLifetime(time.Minute)
	writer.SetMaxOpenConns(cmd.WriterCount)
	writerCollector := sqlstats.NewStatsCollector("target_writer", writer)
	prometheus.MustRegister(writerCollector)
	defer prometheus.Unregister(writerCollector)

	var tablesToDo []string
	for _, t := range tables {
		tablesToDo = append(tablesToDo, t.Name)
	}
	logrus.Infof("starting to clone tables: %v", tablesToDo)

	tablesTotalMetric.Add(float64(len(tables)))
	for _, table := range tables {
		rowCountMetric.WithLabelValues(table.Name).Add(float64(table.EstimatedRows))
	}

	var sourceLimiter core.Limiter
	var targetLimiter core.Limiter
	var writerLimiter core.Limiter
	if cmd.UseConcurrencyLimits {
		sourceLimiter = makeLimiter("source_reader_limiter")
		targetLimiter = makeLimiter("target_reader_limiter")
		writerLimiter = makeLimiter("writer_limiter")
	}

	g, ctx := errgroup.WithContext(ctx)

	tableParallelism := semaphore.NewWeighted(cmd.TableParallelism)

	for _, t := range tables {
		table := t
		err = tableParallelism.Acquire(ctx, 1)
		if err != nil {
			return errors.WithStack(err)
		}
		g.Go(func() error {
			defer tableParallelism.Release(1)

			diffs := make(chan Diff)

			writer := NewWriter(cmd.WriterConfig, table, writer, writerLimiter)
			writer.Write(ctx, g, diffs)

			reader := NewReader(
				cmd.ReaderConfig,
				table,
				sourceReader,
				sourceLimiter,
				targetReader,
				targetLimiter,
			)
			if err != nil {
				return errors.WithStack(err)
			}

			if cmd.NoDiff {
				err = reader.Read(ctx, diffs)
				if err != nil {
					return errors.WithStack(err)
				}
			} else {
				err = reader.Diff(ctx, diffs)
				if err != nil {
					return errors.WithStack(err)
				}
			}

			// All diffing done, close the diffs channel
			close(diffs)

			return nil
		})
	}

	return g.Wait()
}
