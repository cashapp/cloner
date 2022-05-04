package clone

import (
	"context"
	_ "net/http/pprof"
	"time"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

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
		logger.Infof("full checksum done")
	}

	if len(diffs) > 0 {
		cmd.reportDiffs(diffs)
		return errors.Errorf("found diffs")
	} else {
		logger.Infof("no diffs found")
	}
	return errors.WithStack(err)
}

func (cmd *Checksum) reportDiffs(diffs []Diff) {
	type stats struct {
		inserts int64
		deletes int64
		updates int64
	}
	var totalStats stats
	statsByTable := make(map[string]stats)
	for _, diff := range diffs {
		byTable := statsByTable[diff.Row.Table.Name]
		switch diff.Type {
		case Update:
			totalStats.updates++
			byTable.updates++
		case Delete:
			totalStats.deletes++
			byTable.deletes++
		case Insert:
			totalStats.inserts++
			byTable.inserts++
		case Repair:
			panic("Repair diff type not supported here")
		}
		statsByTable[diff.Row.Table.Name] = byTable
		logrus.WithField("table", diff.Row.Table.Name).
			WithField("diff_type", diff.Type.String()).
			Errorf("diff %v %v id=%v", diff.Row.Table.Name, diff.Type, diff.Row.KeyValues())
	}
	logrus.Errorf("total diffs inserts=%d deletes=%d updates=%d", totalStats.inserts, totalStats.deletes, totalStats.updates)
	for table, stat := range statsByTable {
		logrus.WithField("table", table).
			Errorf("'%s' diffs inserts=%d deletes=%d updates=%d", table, stat.inserts, stat.deletes, stat.updates)
	}
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

	diffs := make(chan Diff)

	// Reporter
	var foundDiffs []Diff
	g.Go(func() error {
		for diff := range diffs {
			logrus.WithField("table", diff.Row.Table.Name).
				WithField("diff_type", diff.Type.String()).
				WithField("id", diff.Row.KeyValues()).
				Errorf("diff %v %v id=%v", diff.Row.Table.Name, diff.Type, diff.Row.KeyValues())
			foundDiffs = append(foundDiffs, diff)
		}
		return nil
	})

	g.Go(func() error {
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

				var err error

				reader := NewReader(
					cmd.ReaderConfig,
					table,
					sourceReader,
					sourceLimiter,
					targetReader,
					targetLimiter,
				)

				err = reader.Diff(ctx, diffs)
				if err != nil {
					return errors.WithStack(err)
				}

				return nil
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

	err = g.Wait()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return foundDiffs, err
}
