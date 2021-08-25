package clone

import (
	"context"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	_ "net/http/pprof"
	"time"

	"github.com/pkg/errors"
)

type Checksum struct {
	ReaderConfig
}

// Run finds any differences between source and target
func (cmd *Checksum) Run() error {
	var err error

	start := time.Now()

	err = cmd.ReaderConfig.LoadConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	logrus.Infof("using config: %v", cmd)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	diffs, err := cmd.run(ctx)

	elapsed := time.Since(start)
	logger := logrus.WithField("duration", elapsed)
	if err != nil {
		if stackErr, ok := err.(stackTracer); ok {
			logger = logger.WithField("stacktrace", stackErr.StackTrace())
		}
		logger.WithError(err).Errorf("error: %+v", err)
	} else {
		logger.Infof("full checksum success")
	}

	if len(diffs) > 0 {
		// TODO log a more detailed diff report
		return errors.Errorf("found diffs")
	}
	return errors.WithStack(err)
}

func (cmd *Checksum) run(ctx context.Context) ([]Diff, error) {
	if cmd.TableParallelism == 0 {
		return nil, errors.Errorf("need more parallelism")
	}

	// Load tables
	// TODO in consistent clone we should diff the schema of the source with the target,
	//      for now we just use the target schema
	tables, err := LoadTables(ctx, cmd.ReaderConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	sourceReader, err := cmd.Source.ReaderDB()
	if err != nil {
		return nil, errors.WithStack(err)
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
		return nil, errors.WithStack(err)
	}
	defer targetReader.Close()
	// Refresh connections regularly so they don't go stale
	targetReader.SetConnMaxLifetime(time.Minute)
	targetReader.SetMaxOpenConns(cmd.ReaderCount)
	targetReaderCollector := sqlstats.NewStatsCollector("target_reader", targetReader)
	prometheus.MustRegister(targetReaderCollector)
	defer prometheus.Unregister(targetReaderCollector)

	var tablesToDo []string
	for _, t := range tables {
		tablesToDo = append(tablesToDo, t.Name)
	}
	logrus.Infof("starting to diff tables: %v", tablesToDo)

	tablesTotalMetric.Add(float64(len(tables)))
	for _, table := range tables {
		rowCountMetric.WithLabelValues(table.Name).Add(float64(table.EstimatedRows))
	}

	var sourceLimiter core.Limiter
	var targetLimiter core.Limiter
	if cmd.UseConcurrencyLimits {
		sourceLimiter = makeLimiter("source_reader_limiter")
		targetLimiter = makeLimiter("target_reader_limiter")
	}

	g, ctx := errgroup.WithContext(ctx)

	tableParallelism := semaphore.NewWeighted(cmd.TableParallelism)

	diffs := make(chan Diff)
	// Reporter
	var foundDiffs []Diff
	g.Go(func() error {
		for diff := range diffs {
			foundDiffs = append(foundDiffs, diff)
		}
		return nil
	})

	for _, table := range tables {
		err = tableParallelism.Acquire(ctx, 1)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		g.Go(func() error {
			defer tableParallelism.Release(1)

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

			err = reader.Diff(ctx, diffs)
			if err != nil {
				return errors.WithStack(err)
			}

			return nil
		})
	}

	return foundDiffs, g.Wait()
}
