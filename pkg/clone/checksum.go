package clone

import (
	"context"
	"database/sql"
	"fmt"
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

	IgnoreReplicationLag bool          `help:"Normally replication lag is checked before we start the checksum since the algorithm used assumes low replication lag, passing this flag ignores the check" default:"false"`
	HeartbeatTable       string        `help:"Name of the table to use for heartbeats which emits the real replication lag as the 'replication_lag_seconds' metric" optional:"" default:"_cloner_heartbeat"`
	TaskName             string        `help:"The name of this task is used in heartbeat and checkpoints table as well as the name of the lease, only a single process can run as this task" default:"main"`
	MaxReplicationLag    time.Duration `help:"The maximum replication lag we tolerate, this should be more than the heartbeat frequency used by the replication task" default:"1m"`
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

func (cmd *Checksum) readLag(ctx context.Context, target *sql.DB) (time.Duration, error) {
	retry := RetryOptions{
		Limiter:       nil, // will we ever use concurrency limiter again? probably not?
		AcquireMetric: readLimiterDelay.WithLabelValues("target"),
		MaxRetries:    cmd.ReadRetries,
		Timeout:       cmd.ReadTimeout,
	}

	lag := time.Hour
	err := Retry(ctx, retry, func(ctx context.Context) error {
		stmt := fmt.Sprintf("SELECT time FROM %s WHERE task = ?", cmd.HeartbeatTable)
		row := target.QueryRowContext(ctx, stmt, cmd.TaskName)
		var lastHeartbeat time.Time
		err := row.Scan(&lastHeartbeat)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// We haven't received the first heartbeat yet, maybe we're an hour behind, who knows?
				// We're definitely more than 0 ms so let's go with one hour just to pick a number >0
				lag = time.Hour
			} else {
				return errors.WithStack(err)
			}
		} else {
			lag = time.Now().UTC().Sub(lastHeartbeat)
		}
		return nil
	})
	if err != nil {
		return lag, errors.WithStack(err)
	}
	return lag, nil
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

	if !cmd.IgnoreReplicationLag {
		lag, err := cmd.readLag(ctx, targetReader)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if lag > cmd.MaxReplicationLag {
			return nil, errors.Errorf("replication lag too high: %v, "+
				"this checksum algorithm requires relatively low replication lag to work, "+
				"please try again when replication lag is lower", lag)
		}

		logrus.Infof("replication lag is fine: %v", lag)
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
