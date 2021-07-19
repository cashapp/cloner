package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"hash/fnv"
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
)

func init() {
	prometheus.MustRegister(eventsReceived)
	prometheus.MustRegister(eventsProcessed)
	prometheus.MustRegister(eventsIgnored)
	prometheus.MustRegister(replicationLag)
	prometheus.MustRegister(heartbeatsRead)
}

type Replicate struct {
	WriterConfig

	TaskName string `help:"The name of this task is used in heartbeat and checkpoints table as well as the name of the lease, only a single process can run as this task" required:""`
	ServerID uint32 `help:"Unique identifier of this server, defaults to a hash of the TaskName" optional:""`

	CheckpointTable    string        `help:"Name of the table to use for heartbeats which emits the real replication lag as the 'replication_lag_seconds' metric" optional:"" default:"_cloner_checkpoint"`
	HeartbeatTable     string        `help:"Name of the table to use for heartbeats which emits the real replication lag as the 'replication_lag_seconds' metric" optional:"" default:"_cloner_heartbeat"`
	HeartbeatFrequency time.Duration `help:"How often to to write to the heartbeat table, this will be the resolution of the real replication lag metric, set to 0 if you want to disable heartbeats" default:"30s"`
	CreateTables       bool          `help:"Create the heartbeat table if it does not exist" default:"true"`
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

type Position struct {
	File     string
	Position uint32
	Gset     mysql.GTIDSet
}

func (cmd *Replicate) run(ctx context.Context) error {
	replicator, err := NewReplicator(*cmd)
	if err != nil {
		return errors.WithStack(err)
	}
	return replicator.run(ctx)
}

// Replicator replicates from source to target
type Replicator struct {
	config       Replicate
	syncerCfg    replication.BinlogSyncerConfig
	source       *sql.DB
	sourceSchema string
	target       *sql.DB

	schemaCache map[uint64]*schema.Table

	// tx holds the currently executing target transaction
	tx       *sql.Tx
}

func NewReplicator(config Replicate) (*Replicator, error) {
	var err error
	r := Replicator{config: config, schemaCache: make(map[uint64]*schema.Table)}
	if r.config.ServerID == 0 {
		hasher := fnv.New32()
		_, err = hasher.Write([]byte(r.config.TaskName))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		r.config.ServerID = hasher.Sum32()
	}
	r.sourceSchema, err = r.config.Source.Schema()
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
	g.Go(func() error {
		return r.replicate(ctx)
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
	source, err := r.config.Source.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	err = source.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	r.source = source

	target, err := r.config.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	err = target.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	r.target = target

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

	return nil
}

func (r *Replicator) replicate(ctx context.Context) error {
	// TODO acquire lease, there should only be a single replicator running per source->target pair
	var err error

	r.syncerCfg, err = r.config.Source.BinlogSyncerConfig(r.config.ServerID)
	if err != nil {
		return errors.WithStack(err)
	}
	syncer := replication.NewBinlogSyncer(r.syncerCfg)
	defer syncer.Close()

	position, gset, err := r.readStartingPoint(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	var streamer *replication.BinlogStreamer
	if gset != nil {
		streamer, err = syncer.StartSyncGTID(*gset)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		streamer, err = syncer.StartSync(position)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	var nextPos mysql.Position

	for {
		// TODO restart and retry with back off

		e, err := streamer.GetEvent(ctx)
		if err != nil {
			return errors.WithStack(err)
		}

		eventsReceived.Inc()

		if e.Header.LogPos > 0 {
			// Some events like FormatDescriptionEvent return 0, ignore.
			nextPos.Pos = e.Header.LogPos
		}

		ignored := false
		switch event := e.Event.(type) {
		case *replication.RotateEvent:
			nextPos.Name = string(event.NextLogName)
			nextPos.Pos = uint32(event.Position)
		case *replication.QueryEvent:
			if string(event.Query) == "BEGIN" {
				r.tx, err = r.target.BeginTx(ctx, nil)
				if err != nil {
					return errors.WithStack(err)
				}
			} else {
				ignored = true
			}
		case *replication.RowsEvent:
			if !r.shouldReplicate(event.Table) {
				ignored = true
				continue
			}
			if r.isDelete(e) {
				err := r.deleteRows(ctx, e.Header, event)
				if err != nil {
					return errors.WithStack(err)
				}
			} else {
				err := r.replaceRows(ctx, e.Header, event)
				if err != nil {
					return errors.WithStack(err)
				}
			}
		case *replication.XIDEvent:
			gset := event.GSet
			err := r.writeCheckpoint(ctx, nextPos, gset)
			if err != nil {
				return errors.WithStack(err)
			}
			err = r.tx.Commit()
			if err != nil {
				return errors.WithStack(err)
			}
			r.tx = nil
		default:
			ignored = true
		}

		if ignored {
			eventsIgnored.Inc()
		} else {
			eventsProcessed.Inc()
		}
	}
}

func (r *Replicator) isDelete(e *replication.BinlogEvent) bool {
	return e.Header.EventType == replication.DELETE_ROWS_EVENTv0 || e.Header.EventType == replication.DELETE_ROWS_EVENTv1 || e.Header.EventType == replication.DELETE_ROWS_EVENTv2
}

func (r *Replicator) replaceRows(ctx context.Context, header *replication.EventHeader, event *replication.RowsEvent) error {
	if r.tx == nil {
		return errors.Errorf("transaction was not started with BEGIN")
	}
	tableSchema, err := r.getTableSchema(event.Table)
	if err != nil {
		return errors.WithStack(err)
	}
	tableName := tableSchema.Name
	writeType := writeTypeOfEvent(header)
	timer := prometheus.NewTimer(writeDuration.WithLabelValues(tableName, writeType))
	defer timer.ObserveDuration()
	defer func() {
		if err == nil {
			writesSucceeded.WithLabelValues(tableName, writeType).Add(float64(len(event.Rows)))
		} else {
			writesFailed.WithLabelValues(tableName, writeType).Add(float64(len(event.Rows)))
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

	valueStrings := make([]string, 0, len(event.Rows))
	valueArgs := make([]interface{}, 0, len(event.Rows)*len(tableSchema.Columns))
	for _, row := range event.Rows {
		valueStrings = append(valueStrings, values)
		valueArgs = append(valueArgs, row...)
	}
	// TODO build the entire statement with a strings.Builder like in deleteRows below. For speed.
	stmt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
		tableSchema.Name, columnList, strings.Join(valueStrings, ","))
	// TODO timeout and retries
	_, err = r.tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}

	return nil
}

func writeTypeOfEvent(header *replication.EventHeader) string {
	switch header.EventType {
	case replication.WRITE_ROWS_EVENTv0, replication.WRITE_ROWS_EVENTv1, replication.WRITE_ROWS_EVENTv2:
		return "insert"
	case replication.UPDATE_ROWS_EVENTv0, replication.UPDATE_ROWS_EVENTv1, replication.UPDATE_ROWS_EVENTv2:
		return "update"
	case replication.DELETE_ROWS_EVENTv0, replication.DELETE_ROWS_EVENTv1, replication.DELETE_ROWS_EVENTv2:
		return "delete"
	default:
		logrus.Fatalf("unknown event type: %d", header.EventType)
		panic("unknown event type")
	}
}

func (r *Replicator) shouldReplicate(table *replication.TableMapEvent) bool {
	if r.sourceSchema != string(table.Schema) {
		return false
	}
	if len(r.config.Config.Tables) == 0 {
		// No tables configged, we replicate everything
		return true
	}
	if string(table.Table) == r.config.HeartbeatTable {
		return true
	}
	_, exists := r.config.Config.Tables[string(table.Table)]
	return exists
}

func (r *Replicator) deleteRows(ctx context.Context, header *replication.EventHeader, event *replication.RowsEvent) (err error) {
	if r.tx == nil {
		return errors.Errorf("transaction was not started with BEGIN")
	}

	tableSchema, err := r.getTableSchema(event.Table)
	if err != nil {
		return errors.WithStack(err)
	}
	tableName := tableSchema.Name
	writeType := writeTypeOfEvent(header)
	timer := prometheus.NewTimer(writeDuration.WithLabelValues(tableName, writeType))
	defer timer.ObserveDuration()
	defer func() {
		if err == nil {
			writesSucceeded.WithLabelValues(tableName, writeType).Add(float64(len(event.Rows)))
		} else {
			writesFailed.WithLabelValues(tableName, writeType).Add(float64(len(event.Rows)))
		}
	}()
	var stmt strings.Builder
	args := make([]interface{}, 0, len(event.Rows))
	stmt.WriteString("DELETE FROM `")
	stmt.Write(event.Table.Table)
	stmt.WriteString("` WHERE ")
	for rowIdx, row := range event.Rows {
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

		if rowIdx != len(event.Rows)-1 {
			stmt.WriteString(" OR ")
		}
	}

	stmtString := stmt.String()
	// TODO timeout and retries
	_, err = r.tx.ExecContext(ctx, stmtString, args...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmtString)
	}
	return nil
}

func (r *Replicator) getTableSchema(event *replication.TableMapEvent) (*schema.Table, error) {
	tableSchema, ok := r.schemaCache[event.TableID]
	if !ok {
		var err error
		tableSchema, err = schema.NewTableFromSqlDB(r.source, string(event.Schema), string(event.Table))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// TODO invalidate cache on each DDL event
		r.schemaCache[event.TableID] = tableSchema
	}
	return tableSchema, nil
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
	// TODO retries with backoff?
	timeoutCtx, cancel := context.WithTimeout(ctx, r.config.WriteTimeout)
	defer cancel()
	row := r.source.QueryRowContext(timeoutCtx,
		fmt.Sprintf("SELECT count FROM %s WHERE name = ?", r.config.HeartbeatTable), r.config.TaskName)
	var count int64
	err := row.Scan(&count)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// We haven't written the first heartbeat yet
			count = 0
		} else {
			return errors.WithStack(err)
		}
	}
	_, err = r.source.ExecContext(timeoutCtx,
		fmt.Sprintf("REPLACE INTO %s (name, time, count) VALUES (?, CURRENT_TIMESTAMP(), ?)",
			r.config.HeartbeatTable),
		r.config.TaskName, count+1)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *Replicator) writeCheckpoint(ctx context.Context, position mysql.Position, gset mysql.GTIDSet) error {
	gsetString := ""
	if gset != nil {
		gsetString = gset.String()
	}
	_, err := r.target.ExecContext(ctx,
		fmt.Sprintf("REPLACE INTO %s (name, file, position, gtid_set, timestamp) VALUES (?, ?, ?, ?, CURRENT_TIMESTAMP())",
			r.config.CheckpointTable),
		r.config.TaskName, position.Name, position.Pos, gsetString)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (r *Replicator) readStartingPoint(ctx context.Context) (mysql.Position, *mysql.GTIDSet, error) {
	file, position, executedGtidSet, err := r.readCheckpoint(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			file, position, executedGtidSet, err = r.readMasterPosition(ctx)
			logrus.Infof("starting new replication from current master position %s:%d gtid=%s", file, position, executedGtidSet)
			if err != nil {
				return mysql.Position{}, nil, errors.WithStack(err)
			}
		} else {
			return mysql.Position{}, nil, errors.WithStack(err)
		}
	} else {
		masterFile, masterPos, masterGtidSet, err := r.readMasterPosition(ctx)
		if err != nil {
			return mysql.Position{}, nil, errors.WithStack(err)
		}
		logrus.Infof("re-starting replication from %s:%d gtid=%s (master is currently at %s:%d gtid=%s)",
			file, position, executedGtidSet, masterFile, masterPos, masterGtidSet)
	}

	// We always have a position
	pos := mysql.Position{
		Name: file,
		Pos:  position,
	}
	// We sometimes have a GTIDSet, if not we return nil
	var gset *mysql.GTIDSet
	if executedGtidSet != "" {
		parsed, err := mysql.ParseGTIDSet(r.syncerCfg.Flavor, executedGtidSet)
		if err != nil {
			return mysql.Position{}, nil, errors.WithStack(err)
		}
		gset = &parsed
	}
	return pos, gset, nil
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
	// TODO retries with backoff?
	timeoutCtx, cancel := context.WithTimeout(ctx, r.config.WriteTimeout)
	defer cancel()
	stmt := fmt.Sprintf("SELECT CURRENT_TIMESTAMP() - time FROM %s WHERE name = ?", r.config.HeartbeatTable)
	row := r.target.QueryRowContext(timeoutCtx, stmt, r.config.TaskName)
	var lag time.Duration
	err := row.Scan(&lag)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// We haven't received the first heartbeat yet
			return nil
		} else {
			return errors.WithStack(err)
		}
	}
	replicationLag.Set(float64(lag.Milliseconds()))
	heartbeatsRead.Inc()
	return nil
}
