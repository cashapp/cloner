package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/mightyguava/autotx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"hash/fnv"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/pkg/errors"
)

var (
	eventsReceived = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "replication_events_received",
			Help: "How many events we've received",
		},
	)
	eventsProcessed = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "replication_events_processed",
			Help: "How many events we've processed",
		},
	)
	eventsIgnored = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "replication_events_ignored",
			Help: "How many events we've ignored",
		},
	)
	replicationLag = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "replication_lag",
			Help: "The time in milliseconds between a change applied to source is replicated to the target",
		},
	)
	heartbeatsRead = prometheus.NewCounter(
		prometheus.CounterOpts{
			Name: "heartbeats_read",
			Help: "The number of times we've successfully read heartbeats",
		},
	)
	chunksSnapshotted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chunks_snapshotted",
			Help: "How many chunks has been read, partitioned by table.",
		},
		[]string{"table"},
	)
)

func init() {
	prometheus.MustRegister(eventsReceived)
	prometheus.MustRegister(eventsProcessed)
	prometheus.MustRegister(eventsIgnored)
	prometheus.MustRegister(replicationLag)
	prometheus.MustRegister(heartbeatsRead)
	prometheus.MustRegister(chunksSnapshotted)
}

type Replicate struct {
	WriterConfig

	TaskName string `help:"The name of this task is used in heartbeat and checkpoints table as well as the name of the lease, only a single process can run as this task" required:""`
	ServerID uint32 `help:"Unique identifier of this server, defaults to a hash of the TaskName" optional:""`

	ChunkParallelism int `help:"Number of chunks to snapshot concurrently" default:"10"`

	CheckpointTable    string        `help:"Name of the table to used on the target to save the current position in the replication stream" optional:"" default:"_cloner_checkpoint"`
	WatermarkTable     string        `help:"Name of the table to use to reconcile chunk result sets during snapshot rebuilds" optional:"" default:"_cloner_watermark"`
	HeartbeatTable     string        `help:"Name of the table to use for heartbeats which emits the real replication lag as the 'replication_lag_seconds' metric" optional:"" default:"_cloner_heartbeat"`
	HeartbeatFrequency time.Duration `help:"How often to to write to the heartbeat table, this will be the resolution of the real replication lag metric, set to 0 if you want to disable heartbeats" default:"30s"`
	CreateTables       bool          `help:"Create the required tables if they do not exist" default:"true"`
	ChunkBufferSize    int           `help:"Size of internal queues" default:"100"`
	ReconnectTimeout   time.Duration `help:"How long to try to reconnect after a replication failure (set to 0 to retry forever)" default:"5m"`
}

// Run replicates from source to target
func (cmd *Replicate) Run() error {
	var err error

	start := time.Now()

	err = cmd.ReaderConfig.LoadConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	logrus.Infof("using config: %v", cmd)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = cmd.run(ctx)

	elapsed := time.Since(start)
	logger := logrus.WithField("duration", elapsed)
	if err != nil {
		if stackErr, ok := err.(stackTracer); ok {
			logger = logger.WithField("stacktrace", stackErr.StackTrace())
		}
		logger.WithError(err).Errorf("error: %+v", err)
	}

	return errors.WithStack(err)
}

func (cmd *Replicate) run(ctx context.Context) error {
	replicator, err := NewReplicator(*cmd)
	if err != nil {
		return errors.WithStack(err)
	}
	http.HandleFunc("/snapshot", func(writer http.ResponseWriter, request *http.Request) {
		go func() {
			err := replicator.snapshot(context.Background())
			if err != nil {
				logrus.Errorf("failed to snapshot: %v", err)
			}
		}()
		// TODO return status/errors back to the caller?
		_, _ = writer.Write([]byte(""))
	})

	return replicator.run(ctx)
}

// Replicator replicates from source to target
type Replicator struct {
	config       Replicate
	syncerCfg    replication.BinlogSyncerConfig
	source       *sql.DB
	sourceSchema string
	target       *sql.DB
	tables       []*Table

	sourceRetry RetryOptions
	targetRetry RetryOptions

	snapshotter         *Snapshotter
	transactionStreamer *TransactionStream
}

func NewReplicator(config Replicate) (*Replicator, error) {
	var err error
	r := Replicator{
		config: config,
		sourceRetry: RetryOptions{
			Limiter:       nil, // will we ever use concurrency limiter again? probably not?
			AcquireMetric: readLimiterDelay.WithLabelValues("source"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
		targetRetry: RetryOptions{
			Limiter:       nil, // will we ever use concurrency limiter again? probably not?
			AcquireMetric: readLimiterDelay.WithLabelValues("target"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
	}
	if r.config.ServerID == 0 {
		hasher := fnv.New32()
		_, err = hasher.Write([]byte(r.config.TaskName))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.config.ServerID = hasher.Sum32()
	}
	logrus.Infof("using replication server id: %d", r.config.ServerID)
	r.syncerCfg, err = r.config.Source.BinlogSyncerConfig(r.config.ServerID)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.sourceSchema, err = r.config.Source.Schema()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	source, err := r.config.Source.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.source = source

	target, err := r.config.Target.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.target = target

	r.snapshotter, err = NewSnapshotter(r.config)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &r, nil
}

func (r *Replicator) run(ctx context.Context) error {
	err := r.init(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	g, ctx := errgroup.WithContext(ctx)

	transactions := make(chan Transaction)
	g.Go(func() error {
		position, err := r.readStartingPosition(ctx)
		if err != nil {
			return errors.WithStack(err)
		}

		return r.transactionStreamer.Run(ctx, position, transactions)
	})

	transactionsAfterSnapshot := make(chan Transaction)
	g.Go(func() error {
		return r.snapshotter.Run(ctx, transactions, transactionsAfterSnapshot)
	})

	g.Go(func() error {
		b := backoff.NewExponentialBackOff()
		// We try to re-connect for this amount of time before we give up
		// on Kubernetes that generally means we will get restarted with a backoff
		b.MaxElapsedTime = r.config.ReconnectTimeout
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
			err := r.replicate(ctx, b, transactionsAfterSnapshot)
			if errors.Is(err, context.Canceled) {
				return errors.WithStack(err)
			}
			logrus.WithError(err).Errorf("replication write loop failed, restarting")
			sleepTime := b.NextBackOff()
			if sleepTime == backoff.Stop {
				return errors.Wrapf(err, "failed to reconnect after %v", b.GetElapsedTime())
			}
			time.Sleep(sleepTime)
		}
	})

	if r.config.HeartbeatFrequency > 0 {
		g.Go(func() error {
			return r.heartbeat(ctx)
		})
	}
	err = g.Wait()
	if err != nil {
		return errors.WithStack(err)
	}
	return err
}

func (r *Replicator) init(ctx context.Context) error {
	err := r.source.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	err = r.target.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	if r.config.CreateTables {
		err := r.createHeartbeatTable(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
		err = r.createCheckpointTable(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	err = r.snapshotter.Init(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	r.tables, err = LoadTables(ctx, r.config.ReaderConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	heartbeatTable, err := loadTable(ctx, r.config.ReaderConfig, r.config.Source.Type, r.source, r.sourceSchema, r.config.HeartbeatTable, TableConfig{})
	if err != nil {
		return errors.WithStack(err)
	}
	r.tables = append(r.tables, heartbeatTable)

	watermarkTable, err := loadTable(ctx, r.config.ReaderConfig, r.config.Source.Type, r.source, r.sourceSchema, r.config.WatermarkTable, TableConfig{})
	if err != nil {
		return errors.WithStack(err)
	}
	r.tables = append(r.tables, watermarkTable)

	r.transactionStreamer, err = NewTransactionStreamer(r.config, r.tables)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (r *Replicator) replicate(ctx context.Context, b backoff.BackOff, transactions chan Transaction) error {
	for {
		var transaction Transaction
		select {
		case transaction = <-transactions:
		case <-ctx.Done():
			return ctx.Err()
		}

		err := autotx.Transact(ctx, r.target, func(tx *sql.Tx) error {
			for _, mutation := range transaction.Mutations {
				err := r.handleMutation(ctx, tx, mutation)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			err := r.writeCheckpoint(ctx, tx, transaction.FinalPosition)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}

		// We've committed a transaction, we can reset the backoff
		b.Reset()
	}
}

func (r *Replicator) replaceRows(ctx context.Context, tx *sql.Tx, mutation Mutation) error {
	var err error
	tableSchema := mutation.Table.MysqlTable
	tableName := tableSchema.Name
	writeType := mutation.Type.String()
	timer := prometheus.NewTimer(writeDuration.WithLabelValues(tableName, writeType))
	defer timer.ObserveDuration()
	defer func() {
		if err == nil {
			writesSucceeded.WithLabelValues(tableName, writeType).Add(float64(len(mutation.Rows)))
		} else {
			writesFailed.WithLabelValues(tableName, writeType).Add(float64(len(mutation.Rows)))
		}
	}()
	var questionMarks strings.Builder
	var columnListBuilder strings.Builder
	for i, column := range tableSchema.Columns {
		questionMarks.WriteString("?")
		columnListBuilder.WriteString("`")
		columnListBuilder.WriteString(column.Name)
		columnListBuilder.WriteString("`")
		if i != len(tableSchema.Columns)-1 {
			columnListBuilder.WriteString(",")
			questionMarks.WriteString(",")
		}
	}
	values := fmt.Sprintf("(%s)", questionMarks.String())
	columnList := columnListBuilder.String()

	valueStrings := make([]string, 0, len(mutation.Rows))
	valueArgs := make([]interface{}, 0, len(mutation.Rows)*len(tableSchema.Columns))
	for _, row := range mutation.Rows {
		valueStrings = append(valueStrings, values)
		valueArgs = append(valueArgs, row...)
	}
	// TODO build the entire statement with a strings.Builder like in deleteRows below. For speed.
	stmt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
		tableSchema.Name, columnList, strings.Join(valueStrings, ","))
	_, err = tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}

	return nil
}

func (r *Replicator) deleteRows(ctx context.Context, tx *sql.Tx, mutation Mutation) (err error) {
	tableSchema := mutation.Table.MysqlTable
	tableName := tableSchema.Name
	writeType := mutation.Type.String()
	timer := prometheus.NewTimer(writeDuration.WithLabelValues(tableName, writeType))
	defer timer.ObserveDuration()
	defer func() {
		if err == nil {
			writesSucceeded.WithLabelValues(tableName, writeType).Add(float64(len(mutation.Rows)))
		} else {
			writesFailed.WithLabelValues(tableName, writeType).Add(float64(len(mutation.Rows)))
		}
	}()
	var stmt strings.Builder
	args := make([]interface{}, 0, len(mutation.Rows))
	stmt.WriteString("DELETE FROM `")
	stmt.WriteString(mutation.Table.Name)
	stmt.WriteString("` WHERE ")
	for rowIdx, row := range mutation.Rows {
		stmt.WriteString("(")
		for i, pkIndex := range tableSchema.PKColumns {
			args = append(args, row[pkIndex])

			stmt.WriteString("`")
			stmt.WriteString(tableSchema.Columns[pkIndex].Name)
			stmt.WriteString("` = ?")
			if i != len(tableSchema.PKColumns)-1 {
				stmt.WriteString(" AND ")
			}
		}
		stmt.WriteString(")")

		if rowIdx != len(mutation.Rows)-1 {
			stmt.WriteString(" OR ")
		}
	}

	stmtString := stmt.String()
	_, err = tx.ExecContext(ctx, stmtString, args...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmtString)
	}
	return nil
}

func (r *Replicator) heartbeat(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.config.HeartbeatFrequency):
			err := r.writeHeartbeat(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			err = r.readHeartbeat(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}
}

func (r *Replicator) createHeartbeatTable(ctx context.Context) error {
	// TODO retries with backoff?
	timeoutCtx, cancel := context.WithTimeout(ctx, r.config.WriteTimeout)
	defer cancel()
	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
		  name VARCHAR(255) NOT NULL,
		  time TIMESTAMP NOT NULL,
		  count BIGINT(20) NOT NULL,
		  PRIMARY KEY (name)
		)
		`, r.config.HeartbeatTable)
	_, err := r.source.ExecContext(timeoutCtx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not create heartbeat table in source database:\n%s", stmt)
	}
	_, err = r.target.ExecContext(timeoutCtx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not create heartbeat table in target database:\n%s", stmt)
	}
	return nil
}

func (r *Replicator) createCheckpointTable(ctx context.Context) error {
	// TODO retries with backoff?
	timeoutCtx, cancel := context.WithTimeout(ctx, r.config.WriteTimeout)
	defer cancel()
	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			name      VARCHAR(255) NOT NULL,
			file      VARCHAR(255) NOT NULL,
			position  BIGINT(20)   NOT NULL,
			gtid_set  TEXT,
			timestamp TIMESTAMP    NOT NULL,
			PRIMARY KEY (name)
		)
		`, r.config.CheckpointTable)
	_, err := r.target.ExecContext(timeoutCtx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not create checkpoint table in target database:\n%s", stmt)
	}
	return nil
}

func (r *Replicator) writeHeartbeat(ctx context.Context) error {
	err := Retry(ctx, r.sourceRetry, func(ctx context.Context) error {
		return autotx.Transact(ctx, r.source, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "SET time_zone = \"+00:00\"")
			if err != nil {
				return errors.WithStack(err)
			}

			row := tx.QueryRowContext(ctx,
				fmt.Sprintf("SELECT count FROM %s WHERE name = ?", r.config.HeartbeatTable), r.config.TaskName)
			var count int64
			err = row.Scan(&count)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// We haven't written the first heartbeat yet
					count = 0
				} else {
					return errors.WithStack(err)
				}
			}
			heartbeatTime := time.Now().UTC()
			_, err = tx.ExecContext(ctx,
				fmt.Sprintf("REPLACE INTO %s (name, time, count) VALUES (?, ?, ?)",
					r.config.HeartbeatTable),
				r.config.TaskName, heartbeatTime, count+1)
			return errors.WithStack(err)
		})
	})

	return errors.WithStack(err)
}

func (r *Replicator) writeCheckpoint(ctx context.Context, tx *sql.Tx, position Position) error {
	gsetString := ""
	if position.Gset != nil {
		gsetString = position.Gset.String()
	}
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf("REPLACE INTO %s (name, file, position, gtid_set, timestamp) VALUES (?, ?, ?, ?, ?)",
			r.config.CheckpointTable),
		r.config.TaskName, position.File, position.Position, gsetString, time.Now().UTC())
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *Replicator) readStartingPosition(ctx context.Context) (Position, error) {
	logger := logrus.WithContext(ctx).WithField("task", "replicate")

	file, position, executedGtidSet, err := r.readCheckpoint(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			file, position, executedGtidSet, err = r.readMasterPosition(ctx)
			logger.Infof("starting new replication from current master position %s:%d gtid=%s", file, position, executedGtidSet)
			if err != nil {
				return Position{}, errors.WithStack(err)
			}
		} else {
			return Position{}, errors.WithStack(err)
		}
	} else {
		masterFile, masterPos, masterGtidSet, err := r.readMasterPosition(ctx)
		if err != nil {
			return Position{}, errors.WithStack(err)
		}
		logger.Infof("re-starting replication from %s:%d gtid=%s (master is currently at %s:%d gtid=%s)",
			file, position, executedGtidSet, masterFile, masterPos, masterGtidSet)
	}

	// We sometimes have a GTIDSet, if not we return nil
	var gset mysql.GTIDSet
	if executedGtidSet != "" {
		parsed, err := mysql.ParseGTIDSet(r.syncerCfg.Flavor, executedGtidSet)
		if err != nil {
			return Position{}, errors.WithStack(err)
		}
		gset = parsed
	}
	return Position{
		File:     file,
		Position: position,
		Gset:     gset,
	}, nil
}

func (r *Replicator) readMasterPosition(ctx context.Context) (file string, position uint32, executedGtidSet string, err error) {
	row := r.source.QueryRowContext(ctx, "SHOW MASTER STATUS")
	var binlogDoDB string
	var binlogIgnoreDB string
	err = row.Scan(
		&file,
		&position,
		&binlogDoDB,
		&binlogIgnoreDB,
		&executedGtidSet,
	)
	return
}

func (r *Replicator) readCheckpoint(ctx context.Context) (file string, position uint32, executedGtidSet string, err error) {
	row := r.target.QueryRowContext(ctx,
		fmt.Sprintf("SELECT file, position, gtid_set FROM %s WHERE name = ?",
			r.config.CheckpointTable),
		r.config.TaskName)
	err = row.Scan(
		&file,
		&position,
		&executedGtidSet,
	)
	return
}

func (r *Replicator) readHeartbeat(ctx context.Context) error {
	logger := logrus.WithContext(ctx).WithField("task", "heartbeat")

	err := Retry(ctx, r.targetRetry, func(ctx context.Context) error {
		stmt := fmt.Sprintf("SELECT time FROM %s WHERE name = ?", r.config.HeartbeatTable)
		row := r.target.QueryRowContext(ctx, stmt, r.config.TaskName)
		var lastHeartbeat time.Time
		err := row.Scan(&lastHeartbeat)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// We haven't received the first heartbeat yet
				return nil
			} else {
				return errors.WithStack(err)
			}
		}
		lag := time.Now().UTC().Sub(lastHeartbeat)
		replicationLag.Set(float64(lag.Milliseconds()))
		heartbeatsRead.Inc()
		return nil
	})
	if err != nil {
		logger.WithError(err).Errorf("failed to read heartbeat: %v", err)
	}
	// Heartbeat errors are ignored
	return nil
}

func (r *Replicator) handleMutation(ctx context.Context, tx *sql.Tx, mutation Mutation) error {
	if mutation.Type == Delete {
		err := r.deleteRows(ctx, tx, mutation)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		err := r.replaceRows(ctx, tx, mutation)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (r *Replicator) snapshot(ctx context.Context) error {
	return r.snapshotter.snapshot(ctx)
}
