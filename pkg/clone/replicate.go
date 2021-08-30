package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/mightyguava/autotx"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"hash/fnv"
	"net/http"
	_ "net/http/pprof"
	"sort"
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

	ChunkParallelism int64 `help:"Number of chunks to snapshot concurrently" default:"10"`

	CheckpointTable    string        `help:"Name of the table to used on the target to save the current position in the replication stream" optional:"" default:"_cloner_checkpoint"`
	WatermarkTable     string        `help:"Name of the table to use to reconcile chunk result sets during snapshot rebuilds" optional:"" default:"_cloner_watermark"`
	HeartbeatTable     string        `help:"Name of the table to use for heartbeats which emits the real replication lag as the 'replication_lag_seconds' metric" optional:"" default:"_cloner_heartbeat"`
	HeartbeatFrequency time.Duration `help:"How often to to write to the heartbeat table, this will be the resolution of the real replication lag metric, set to 0 if you want to disable heartbeats" default:"30s"`
	CreateTables       bool          `help:"Create the heartbeat table if it does not exist" default:"true"`
}

var currentReplicator *Replicator

// Run replicates from source to target
func (cmd *Replicate) Run() error {
	http.HandleFunc("/snapshot", func(writer http.ResponseWriter, request *http.Request) {
		go func() {
			if currentReplicator == nil {
				return
			}
			err := currentReplicator.snapshot(context.Background())
			if err != nil {
				logrus.Errorf("failed to snapshot: %v", err)
			}
		}()
		_, _ = writer.Write([]byte(""))
	})

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
	currentReplicator = replicator
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

	sourceRetry RetryOptions
	targetRetry RetryOptions

	// chunks hand over chunks from the SnapshotReader to the main replication thread
	chunks chan OngoingChunk

	// tx holds the currently executing target transaction, only access from the replication thread
	tx *sql.Tx

	// ongoingChunks holds the currently ongoing chunks, only access from the replication thread
	ongoingChunks []*OngoingChunk
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
		schemaCache: make(map[uint64]*schema.Table),
		chunks:      make(chan OngoingChunk),
	}
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
		err = r.createWatermarkTable(ctx)
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
		r.receiveAllOngoingChunks()

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
				err = r.startTransaction(ctx)
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
			err := r.handleRowsEvent(ctx, e, event)
			if err != nil {
				return errors.WithStack(err)
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

func (r *Replicator) startTransaction(ctx context.Context) (err error) {
	r.tx, err = r.target.BeginTx(ctx, nil)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = r.target.ExecContext(ctx, "SET time_zone = \"+00:00\"")
	if err != nil {
		return errors.WithStack(err)
	}
	return err
}

// receiveAllOngoingChunks receives all the read chunks for the SnapshotReader
func (r *Replicator) receiveAllOngoingChunks() {
	for {
		select {
		case chunk := <-r.chunks:
			r.ongoingChunks = append(r.ongoingChunks, &chunk)
		default:
			return
		}
	}
}

func isDelete(eventType replication.EventType) bool {
	return eventType == replication.DELETE_ROWS_EVENTv0 || eventType == replication.DELETE_ROWS_EVENTv1 || eventType == replication.DELETE_ROWS_EVENTv2
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
	if string(table.Table) == r.config.WatermarkTable {
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

func (r *Replicator) createWatermarkTable(ctx context.Context) error {
	// TODO retries with backoff?
	timeoutCtx, cancel := context.WithTimeout(ctx, r.config.WriteTimeout)
	defer cancel()
	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         BIGINT(20)   NOT NULL AUTO_INCREMENT,
			table_name VARCHAR(255) NOT NULL,
			chunk_seq  BIGINT(20)   NOT NULL,
			low        TINYINT      DEFAULT 0,
			high       TINYINT      DEFAULT 0,
			PRIMARY KEY (id)
		)
		`, r.config.WatermarkTable)
	_, err := r.source.ExecContext(timeoutCtx, stmt)
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

func (r *Replicator) writeCheckpoint(ctx context.Context, position mysql.Position, gset mysql.GTIDSet) error {
	gsetString := ""
	if gset != nil {
		gsetString = gset.String()
	}
	_, err := r.target.ExecContext(ctx,
		fmt.Sprintf("REPLACE INTO %s (name, file, position, gtid_set, timestamp) VALUES (?, ?, ?, ?, ?)",
			r.config.CheckpointTable),
		r.config.TaskName, position.Name, position.Pos, gsetString, time.Now().UTC())
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
	timeoutCtx, cancel := context.WithTimeout(ctx, r.config.ReadTimeout)
	defer cancel()
	stmt := fmt.Sprintf("SELECT time FROM %s WHERE name = ?", r.config.HeartbeatTable)
	row := r.target.QueryRowContext(timeoutCtx, stmt, r.config.TaskName)
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
}

// OngoingChunk is a mutable struct for representing the current reconciliation state of a chunk, it is used single
// threaded by the replication thread only
type OngoingChunk struct {
	InsideWatermarks bool
	Rows             []*Row
	Chunk            Chunk
}

func (c *OngoingChunk) findRow(row []interface{}) (*Row, int, error) {
	n := len(c.Rows)
	i := sort.Search(n, func(i int) bool {
		return c.Rows[i].PkAfterOrEqual(row)
	})
	var candidate *Row
	if n == i {
		i = -1
	} else {
		candidate = c.Rows[i]
		if !candidate.PkEqual(row) {
			candidate = nil
		}
	}
	return candidate, i, nil
}

func (c *OngoingChunk) updateRow(i int, row []interface{}) {
	c.Rows[i] = c.Rows[i].Updated(row)
}

func (c *OngoingChunk) deleteRow(i int) {
	c.Rows = append(c.Rows[:i], c.Rows[i+1:]...)
}

func (c *OngoingChunk) insertRow(i int, row []interface{}) {
	pk := c.Chunk.Table.PkOfRow(row)
	r := &Row{
		Table: c.Chunk.Table,
		ID:    pk,
		Data:  row,
	}
	if i == -1 {
		// We found no place to insert it so we append it
		c.Rows = append(c.Rows, r)
	} else {
		c.Rows = append(c.Rows[:i], append([]*Row{r}, c.Rows[i:]...)...)
	}
}

type SnapshotReader struct {
	config      Replicate
	source      *sql.DB
	sourceRetry RetryOptions
}

func NewSnapshotReader(config Replicate, source *sql.DB) *SnapshotReader {
	return &SnapshotReader{
		config: config,
		source: source,
		sourceRetry: RetryOptions{
			Limiter:       nil, // will we ever use concurrency limiter again? probably not?
			AcquireMetric: readLimiterDelay.WithLabelValues("source"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
	}
}

func (r *SnapshotReader) snapshot(ctx context.Context, chunkChan chan OngoingChunk) error {
	g, ctx := errgroup.WithContext(ctx)

	tables, err := LoadTables(ctx, r.config.ReaderConfig)
	if err != nil {
		return errors.WithStack(err)
	}
	logrus.Infof("starting snapshot")

	tableParallelism := semaphore.NewWeighted(r.config.TableParallelism)

	chunks := make(chan Chunk)

	chunkParallelism := semaphore.NewWeighted(r.config.ChunkParallelism)

	// Start the goroutine that will process the chunks
	g.Go(func() error {
		for c := range chunks {
			chunk := c
			err = chunkParallelism.Acquire(ctx, 1)
			if err != nil {
				return errors.WithStack(err)
			}
			g.Go(func() error {
				defer chunkParallelism.Release(1)
				return r.snapshotChunk(ctx, chunk, chunkChan)
			})
		}
		return nil
	})

	// Generate chunks
	g.Go(func() error {
		g, ctx := errgroup.WithContext(ctx)

		for _, t := range tables {
			table := t
			err = tableParallelism.Acquire(ctx, 1)
			if err != nil {
				return errors.WithStack(err)
			}
			g.Go(func() error {
				defer tableParallelism.Release(1)

				tableChunks, err := generateTableChunks2(ctx, table, r.source, r.sourceRetry)
				for _, chunk := range tableChunks {
					chunks <- chunk
				}
				if err != nil {
					return errors.WithStack(err)
				}

				return nil
			})
		}

		err := g.Wait()
		close(chunks)
		return errors.WithStack(err)
	})

	err = g.Wait()
	if err != nil {
		logrus.Infof("snapshot reads success")
	}
	return err

	// load tables to snapshot
	// for each table:
	//   generate chunks
	//   for each chunk:
	//     in source tx:
	//       write low watermark
	//       read all chunk rows
	//       send chunk to main replicate loop
	//     in source tx:
	//       write high watermark

	// changes to main replicate loop
	//   non-blocking receive on chunk channel:
	//     add chunk to local list of currently ongoing chunks in an OngoingChunk struct instance
	//   if event is low watermark:
	//     find matching OngoingChunk:
	//       flip a bit that we're now inside the low/high watermark
	//     if matching OngoingChunk not found:
	//       log an error (this should only happen if the cloner had a crash during a snapshot)
	//   for each row in a RowEvent (RowEvent is also applied as normal):
	//     for each row in RowEvent:
	//       find matching OngoingChunk
	//       index rows in RowEvent by primary key (memoize)
	//       for each row in OngoingChunk:
	//         find row in event (using index):
	//           if delete:
	//             remove row from OngoingChunk
	//           if update:
	//             update the row in OngoingChunk
	//           if insert:
	//             insert row in OngoingChunk (at the correct place)
	//   if event is high watermark:
	//     find matching OngoingChunk:
	//       diff and apply it
	//     if matching OngoingChunk not found:
	//       log an error (this should only happen if the cloner had a crash during a snapshot)
}

/*
 */

func (r *SnapshotReader) snapshotChunk(ctx context.Context, chunk Chunk, chunks chan OngoingChunk) error {
	// First transaction:
	//   1. insert the low watermark
	//   2. read the entire chunk
	//   3. send it to the main replicator thread
	//   4. commit the transaction
	// Given that the low watermark write, the read and the send is in the same transaction the replicator thread
	// should not be able to miss the low watermark.
	err := Retry(ctx, r.sourceRetry, func(ctx context.Context) error {
		return autotx.Transact(ctx, r.source, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx,
				fmt.Sprintf("INSERT INTO %s (table_name, chunk_seq, low) VALUES (?, ?, 1)",
					r.config.WatermarkTable),
				chunk.Table.Name, chunk.Seq)
			if err != nil {
				return errors.WithStack(err)
			}
			stream, err := bufferChunk(ctx, r.sourceRetry, tx, "source", chunk)
			if err != nil {
				return errors.WithStack(err)
			}
			rows, err := readAll(stream)
			if err != nil {
				return errors.WithStack(err)
			}
			chunks <- OngoingChunk{Chunk: chunk, Rows: rows}
			return nil
		})
	})
	if err != nil {
		return errors.WithStack(err)
	}

	// Second transaction: ,//   1. insert the high watermark
	//   2. commit the transaction
	return Retry(ctx, r.sourceRetry, func(ctx context.Context) error {
		return autotx.Transact(ctx, r.source, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx,
				fmt.Sprintf("INSERT INTO %s (table_name, chunk_seq, high) VALUES (?, ?, 1)",
					r.config.WatermarkTable),
				chunk.Table.Name, chunk.Seq)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	})
}

// writeChunk synchronously diffs and writes the chunk to the target (diff and write)
// the writes are made synchronously in the replication stream to maintain strong consistency
func (r *Replicator) writeChunk(ctx context.Context, chunk *OngoingChunk) error {
	logrus.Infof("writing chunk %d-%d", chunk.Chunk.Start, chunk.Chunk.End)
	targetStream, err := bufferChunk(ctx, r.targetRetry, r.target, "target", chunk.Chunk)
	if err != nil {
		return errors.WithStack(err)
	}

	// Diff the streams
	diffs, err := StreamDiff(ctx, chunk.Chunk.Table, stream(chunk.Rows), targetStream)
	if err != nil {
		return errors.WithStack(err)
	}
	logrus.Infof("diffs: %v", len(diffs))

	// Batch up the diffs
	batches, err := BatchTableWrites2(ctx, diffs)
	if err != nil {
		return errors.WithStack(err)
	}
	logrus.Infof("batches: %v", len(batches))

	writeCount := 0

	// Write every batch
	for _, batch := range batches {
		writeCount += len(batch.Rows)
		err := writeBatch(ctx, r.config.WriterConfig, batch, r.target, r.targetRetry)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	logrus.Infof("chunk rows written: %v", writeCount)

	return nil
}

// reconcileOngoingChunks reconciles any ongoing chunks with the changes in the binlog event
func (r *Replicator) reconcileOngoingChunks(e *replication.BinlogEvent, event *replication.RowsEvent) error {
	// TODO this method deserves a LOT of tests!
	// should be O(<rows in the RowsEvent> * lg <rows in the chunk>) given that we can binary chop into chunk
	// RowsEvent is usually not that large so I don't think we need to index anything, that will probably be slower
	tableSchema, err := r.getTableSchema(event.Table)
	if err != nil {
		return errors.WithStack(err)
	}
	for _, chunk := range r.ongoingChunks {
		err = chunk.reconcileBinlogEvent(e, event, tableSchema)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// reconcileBinlogEvent will apply the row
func (c *OngoingChunk) reconcileBinlogEvent(e *replication.BinlogEvent, event *replication.RowsEvent, tableSchema *schema.Table) error {
	if !c.InsideWatermarks {
		return nil
	}
	if c.Chunk.Table.MysqlTable.Name != tableSchema.Name {
		return nil
	}
	if c.Chunk.Table.MysqlTable.Schema != tableSchema.Schema {
		return nil
	}
	isDelete := isDelete(e.Header.EventType)
	if isDelete {
		for _, row := range event.Rows {
			if !c.Chunk.ContainsRow(row) {
				// The row is outside of our range, we can skip it
				continue
			}
			logrus.Infof("reconciling deleted row with ongoing chunk: %v", row)
			// find the row using binary chop (the chunk rows are sorted)
			existingRow, i, err := c.findRow(row)
			if existingRow == nil {
				// Row already deleted, this event probably happened after the low watermark but before the chunk read
			} else {
				if err != nil {
					return errors.WithStack(err)
				}
				c.deleteRow(i)
			}
		}
	} else {
		for _, row := range event.Rows {
			if !c.Chunk.ContainsRow(row) {
				// The row is outside of our range, we can skip it
				continue
			}
			existingRow, i, err := c.findRow(row)
			if err != nil {
				return errors.WithStack(err)
			}
			if existingRow == nil {
				logrus.Infof("reconciling inserted row with ongoing chunk: %v", row)
				// This is either an insert or an update of a row that is deleted after the low watermark but before
				// the chunk read, either way we just insert it and if the delete event comes we take it away again
				c.insertRow(i, row)
			} else {
				logrus.Infof("reconciling updated row with ongoing chunk: %v", row)
				// We found a matching row, it must be an update
				c.updateRow(i, row)
			}
		}
	}
	return nil
}

func (r *Replicator) handleRowsEvent(ctx context.Context, e *replication.BinlogEvent, event *replication.RowsEvent) error {
	wasWatermark, err := r.handleWatermark(ctx, e, event)
	if err != nil {
		return errors.WithStack(err)
	}
	if wasWatermark {
		// We don't otherwise process watermarks
		return nil
	}

	err = r.reconcileOngoingChunks(e, event)
	if err != nil {
		return errors.WithStack(err)
	}

	if isDelete(e.Header.EventType) {
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

	return nil
}

func (r *Replicator) findOngoingChunkFromWatermark(event *replication.RowsEvent) (*OngoingChunk, error) {
	tableSchema, err := r.getTableSchema(event.Table)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	tableNameI, err := tableSchema.GetColumnValue("table_name", event.Rows[0])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	tableName := tableNameI.(string)
	chunkSeqI, err := tableSchema.GetColumnValue("chunk_seq", event.Rows[0])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	chunkSeq := chunkSeqI.(int64)
	for _, chunk := range r.ongoingChunks {
		if chunk.Chunk.Seq == chunkSeq && chunk.Chunk.Table.Name == tableName {
			return chunk, nil
		}
	}
	return nil, nil
}

func (r *Replicator) removeOngoingChunk(chunk *OngoingChunk) {
	n := 0
	for _, x := range r.ongoingChunks {
		if x != chunk {
			r.ongoingChunks[n] = x
			n++
		}
	}
	r.ongoingChunks = r.ongoingChunks[:n]
}

func (r *Replicator) snapshot(ctx context.Context) error {
	// TODO prevent multiple snapshots from running at the same time (the second call should be a no op)
	reader := NewSnapshotReader(r.config, r.source)
	return reader.snapshot(ctx, r.chunks)
}

func (r *Replicator) handleWatermark(ctx context.Context, e *replication.BinlogEvent, event *replication.RowsEvent) (bool, error) {
	tableSchema, err := r.getTableSchema(event.Table)
	if err != nil {
		return false, errors.WithStack(err)
	}
	if tableSchema.Name != r.config.WatermarkTable {
		return false, nil
	}
	if isDelete(e.Header.EventType) {
		// Someone is probably just cleaning out the watermark table
		return true, nil
	}
	if len(event.Rows) != 1 {
		return true, errors.Errorf("more than a single row was written to the watermark table at the same time")
	}
	row := event.Rows[0]
	low, err := tableSchema.GetColumnValue("low", row)
	if err != nil {
		return true, errors.WithStack(err)
	}
	high, err := tableSchema.GetColumnValue("high", row)
	if err != nil {
		return true, errors.WithStack(err)
	}
	if low.(int8) == 1 {
		ongoingChunk, err := r.findOngoingChunkFromWatermark(event)
		if err != nil {
			return true, errors.WithStack(err)
		}
		if ongoingChunk != nil {
			ongoingChunk.InsideWatermarks = true
		}
	}
	if high.(int8) == 1 {
		ongoingChunk, err := r.findOngoingChunkFromWatermark(event)
		if err != nil {
			return true, errors.WithStack(err)
		}
		if ongoingChunk != nil {
			ongoingChunk.InsideWatermarks = false
			err = r.writeChunk(ctx, ongoingChunk)
			if err != nil {
				return true, errors.WithStack(err)
			}
			r.removeOngoingChunk(ongoingChunk)
			if len(r.ongoingChunks) == 0 {
				logrus.Infof("all snapshot chunks written: %v", len(r.ongoingChunks))
			}
		}
	}
	return true, nil
}
