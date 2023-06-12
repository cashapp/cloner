package clone

import (
	"context"
	"database/sql"
	"fmt"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/pkg/errors"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Checksum struct {
	ReaderConfig

	HeartbeatTable                 string        `help:"Name of the table to use for heartbeats which emits the real replication lag as the 'replication_lag_seconds' metric" optional:"" default:"_cloner_heartbeat"`
	TaskName                       string        `help:"The name of this task is used in heartbeat and checkpoints table as well as the name of the lease, only a single process can run as this task" default:"main"`
	IgnoreReplicationLag           bool          `help:"Normally replication lag is checked before we start the checksum since the algorithm used assumes low replication lag, passing this flag ignores the check" default:"false"`
	MaxReplicationLag              time.Duration `help:"The maximum replication lag we tolerate, this should be more than the heartbeat frequency used by the replication task" default:"1m"`
	MaxReplicationLagCheckInterval time.Duration `help:"Maximum interval to check replication lag" default:"1m"`
	RepairAttempts                 int           `help:"How many times to try to repair diffs that are found" default:"0"`

	WriteRetries uint64        `help:"Number of retries" default:"5"`
	WriteTimeout time.Duration `help:"Timeout for each write" default:"30s"`
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
		if cmd.RepairAttempts > 0 {
			diffs, err = cmd.repairDiffs(ctx, diffs)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		// did the repair succeed?
		if len(diffs) > 0 {
			cmd.reportDiffs(diffs)
			err := errors.Errorf("found diffs")
			logger.WithError(err).Infof("found diffs")
			return err
		}
	} else {
		logger.Infof("no diffs found")
	}
	return errors.WithStack(err)
}

func (cmd *Checksum) repairDiffs(ctx context.Context, diffs []Diff) ([]Diff, error) {
	repairer, err := NewRepairer(cmd)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return repairer.repair(ctx, diffs)
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
		var targetData []interface{}
		if diff.Target != nil {
			targetData = diff.Target.Data
		}
		logrus.WithField("table", diff.Row.Table.Name).
			WithField("diff_type", diff.Type.String()).
			Errorf("diff %v %v id=%v source=%v target=%v", diff.Row.Table.Name, diff.Type, diff.Row.KeyValues(), diff.Row.Data, targetData)
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

	logger := logrus.WithField("task", "checksum")
	if !cmd.IgnoreReplicationLag {
		for {
			delay := cmd.MaxReplicationLagCheckInterval
			lag, err := cmd.readLag(ctx, targetReader)
			if err != nil {
				logger.WithError(err).Warnf("failed to check replication lag, checking again in %v", delay)
				continue
			}
			if lag < cmd.MaxReplicationLag {
				logger.Infof("replication lag %v below %v",
					lag, cmd.MaxReplicationLag)
				break
			}

			delay = durationMin(lag, cmd.MaxReplicationLagCheckInterval)

			logger.Infof("replication lag %v is still above %v, checking again in %v",
				lag, cmd.MaxReplicationLag, delay)
			time.Sleep(delay)
		}
	}

	var tablesToDo []string
	for _, t := range tables {
		tablesToDo = append(tablesToDo, t.Name)
	}
	logger.Infof("starting to diff tables: %v", tablesToDo)

	var estimatedRows int64
	tablesTotalMetric.Set(float64(len(tables)))
	for _, table := range tables {
		estimatedRows += table.EstimatedRows
		rowCountMetric.WithLabelValues(table.Name).Set(float64(table.EstimatedRows))
	}

	var sourceLimiter core.Limiter
	var targetLimiter core.Limiter
	if cmd.UseConcurrencyLimits {
		sourceLimiter = makeLimiter("source_reader_limiter")
		targetLimiter = makeLimiter("target_reader_limiter")
	}

	readLogger := NewThroughputLogger("read", cmd.ThroughputLoggingFrequency, uint64(estimatedRows))

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
		g.SetLimit(cmd.TableParallelism)

		for _, t := range tables {
			table := t
			if err != nil {
				return errors.WithStack(err)
			}
			g.Go(func() error {
				var err error

				reader := NewReader(
					cmd.ReaderConfig,
					table,
					readLogger,
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

type Repairer struct {
	config *Checksum
	source *sql.DB
	target *sql.DB
}

func NewRepairer(config *Checksum) (*Repairer, error) {
	source, err := config.Source.ReaderDB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	target, err := config.Target.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Repairer{
		config: config,
		source: source,
		target: target,
	}, nil
}

func (r *Repairer) repairDiffs(ctx context.Context, diffs []Diff) ([]Diff, error) {
	var newDiffs []Diff
	newDiffs = nil
	for _, diff := range diffs {
		newDiff, err := r.repairDiff(ctx, diff)
		if err != nil {
			return newDiffs, errors.WithStack(err)
		}
		if newDiff != nil {
			newDiffs = append(newDiffs, *newDiff)
		}
	}
	return newDiffs, nil
}

func (r *Repairer) repairDiff(ctx context.Context, diff Diff) (newDiff *Diff, err error) {
	retry := RetryOptions{
		Limiter:       nil, // will we ever use concurrency limiter again? probably not?
		AcquireMetric: writeLimiterDelay,
		MaxRetries:    r.config.WriteRetries,
		Timeout:       r.config.WriteTimeout,
	}
	err = Retry(ctx, retry, func(ctx context.Context) error {
		switch diff.Type {
		case Update:
			row, err := r.readRow(ctx, diff)
			if err != nil {
				return errors.WithStack(err)
			}
			if row == nil {
				newDiff = &Diff{
					Type:   Delete,
					Row:    diff.Row,
					Target: nil,
				}
				return nil
			}
			err = r.writeRow(ctx, row)
			if err != nil {
				return errors.WithStack(err)
			}
		case Insert:
			err := r.writeRow(ctx, diff.Row)
			if err != nil {
				return errors.WithStack(err)
			}
		case Delete:
			err := r.deleteRow(ctx, diff)
			if err != nil {
				return errors.WithStack(err)
			}
		default:
			panic(fmt.Sprintf("can't repair %s", diff.Type.String()))
		}

		newDiff, err = r.rediff(ctx, diff)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	return newDiff, errors.WithStack(err)
}

func (r *Repairer) rediff(ctx context.Context, diff Diff) (*Diff, error) {
	sourceRow, err := r.readRow(ctx, diff)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	targetRow, err := r.readRow(ctx, diff)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if sourceRow == nil && targetRow != nil {
		return &Diff{
			Type:   Delete,
			Row:    targetRow,
			Target: nil,
		}, nil
	} else if sourceRow != nil && targetRow == nil {
		return &Diff{
			Type:   Insert,
			Row:    sourceRow,
			Target: nil,
		}, nil
	} else if sourceRow == nil && targetRow == nil {
		return nil, nil
	} else {
		rowsEqual, err := RowsEqual(sourceRow, targetRow)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if rowsEqual {
			return nil, nil
		} else {
			return &Diff{
				Type:   Update,
				Row:    sourceRow,
				Target: targetRow,
			}, nil
		}
	}
}

func (r *Repairer) readRow(ctx context.Context, diff Diff) (*Row, error) {
	table := diff.Row.Table
	var whereClause strings.Builder
	for i, column := range table.KeyColumns {
		if i > 0 {
			whereClause.WriteString(" AND ")
		}
		whereClause.WriteString("`")
		whereClause.WriteString(column)
		whereClause.WriteString("`")
		whereClause.WriteString(" = ?")
	}
	stmt := fmt.Sprintf("SELECT %s FROM %s WHERE %s",
		table.ColumnList, table.Name, whereClause.String())
	rows, err := r.source.QueryContext(ctx, stmt, diff.Row.KeyValues()...)
	if err != nil {
		return nil, errors.Wrapf(err, "could not execute: %s", stmt)
	}
	defer rows.Close()
	if !rows.Next() {
		return nil, nil
	}
	data := make([]interface{}, len(table.Columns))

	scanArgs := make([]interface{}, len(data))
	for i := range data {
		scanArgs[i] = &data[i]
	}
	err = rows.Scan(scanArgs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// We replaced the data in the row slice with pointers to the local vars, so lets put this back after the read
	return &Row{
		Table: table,
		Data:  data,
	}, nil
}

func (r *Repairer) writeRow(ctx context.Context, row *Row) error {
	var err error
	table := row.Table
	tableSchema := table.MysqlTable
	var questionMarks strings.Builder
	var columnListBuilder strings.Builder
	for i, column := range tableSchema.Columns {
		if table.IgnoredColumnsBitmap[i] {
			continue
		}
		if i != 0 {
			columnListBuilder.WriteString(",")
			questionMarks.WriteString(",")
		}
		questionMarks.WriteString("?")
		columnListBuilder.WriteString("`")
		columnListBuilder.WriteString(column.Name)
		columnListBuilder.WriteString("`")
	}
	values := fmt.Sprintf("(%s)", questionMarks.String())
	columnList := columnListBuilder.String()

	args := make([]interface{}, 0, len(tableSchema.Columns))
	for i, val := range row.Data {
		if !table.IgnoredColumnsBitmap[i] {
			args = append(args, val)
		}
	}
	stmt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
		tableSchema.Name, columnList, values)
	_, err = r.target.ExecContext(ctx, stmt, args...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}

	return nil
}

func (r *Repairer) deleteRow(ctx context.Context, diff Diff) error {
	table := diff.Row.Table
	var whereClause strings.Builder
	for i, column := range table.KeyColumns {
		if i > 0 {
			whereClause.WriteString(" AND ")
		}
		whereClause.WriteString("`")
		whereClause.WriteString(column)
		whereClause.WriteString("`")
		whereClause.WriteString(" = ?")
	}
	args := diff.Row.KeyValues()
	_, err := r.target.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE %s",
		table.Name, whereClause.String()), args...)
	if err != nil {
		return errors.WithStack(err)
	}
	// We replaced the data in the row slice with pointers to the local vars, so lets put this back after the read
	return nil
}

func (r *Repairer) repair(ctx context.Context, diffs []Diff) ([]Diff, error) {
	logrus.Infof("rechecking %d diffs", len(diffs))
	diffs, err := r.rediffAll(ctx, diffs)
	if err != nil {
		return diffs, errors.WithStack(err)
	}
	if len(diffs) == 0 {
		logrus.Infof("all diffs good after recheck")
		logrus.Infof("no diffs found")
		return nil, nil
	}
	for i := 0; i < r.config.RepairAttempts; i++ {
		logrus.Warnf("found diffs")
		r.config.reportDiffs(diffs)
		logrus.Infof("repair attempt %d out of %d", i+1, r.config.RepairAttempts)
		diffs, err := r.repairDiffs(ctx, diffs)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if len(diffs) == 0 {
			logrus.Infof("all diffs repaired")
			return nil, nil
		}
	}
	return diffs, nil
}

func (r *Repairer) rediffAll(ctx context.Context, diffs []Diff) ([]Diff, error) {
	var newDiffs []Diff
	for _, diff := range diffs {
		newDiff, err := r.rediff(ctx, diff)
		if err != nil {
			return diffs, errors.WithStack(err)
		}
		if newDiff != nil {
			newDiffs = append(newDiffs, *newDiff)
		}
	}
	return newDiffs, nil
}
