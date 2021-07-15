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
)

type Replicate struct {
	WriterConfig
}

// Run finds any differences between source and target
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

type MasterStatus struct {
	File            string
	Position        uint32
	BinlogDoDB      string
	BinlogIgnoreDB  string
	ExecutedGtidSet string
}

func (cmd *Replicate) run(ctx context.Context) error {
	replicator := NewReplicator(cmd.WriterConfig)
	return replicator.run(ctx)
}

func NewReplicator(config WriterConfig) *Replicator {
	return &Replicator{config: config, schemaCache: make(map[uint64]*schema.Table)}
}

// Replicator replicates from source to target
type Replicator struct {
	config WriterConfig
	syncer *replication.BinlogSyncer
	source *sql.DB
	target *sql.DB

	// tx holds the currently executing target transaction
	tx          *sql.Tx
	schemaCache map[uint64]*schema.Table
}

func (r *Replicator) run(ctx context.Context) error {
	// TODO acquire lease, there should only be a single replicator running per source->target pair

	source, err := r.config.Source.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	r.source = source
	target, err := r.config.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	r.target = source

	row := source.QueryRowContext(ctx, "SHOW MASTER STATUS")
	masterStatus := MasterStatus{}
	err = row.Scan(
		&masterStatus.File,
		&masterStatus.Position,
		&masterStatus.BinlogDoDB,
		&masterStatus.BinlogIgnoreDB,
		&masterStatus.ExecutedGtidSet,
	)
	if err != nil {
		return errors.WithStack(err)
	}

	syncerCfg, err := r.config.Source.BinlogSyncerConfig()
	if err != nil {
		return errors.WithStack(err)
	}
	r.syncer = replication.NewBinlogSyncer(syncerCfg)

	streamer, err := r.syncer.StartSync(mysql.Position{Name: masterStatus.File, Pos: masterStatus.Position})
	if err != nil {
		return errors.WithStack(err)
	}

	for {
		e, err := streamer.GetEvent(ctx)
		if err != nil {
			return errors.WithStack(err)
		}

		eventsReceived.Inc()

		ignored := false
		switch event := e.Event.(type) {
		case *replication.QueryEvent:
			if string(event.Query) == "BEGIN" {
				r.tx, err = target.BeginTx(ctx, nil)
				if err != nil {
					return errors.WithStack(err)
				}
			}
		case *replication.RowsEvent:
			if !r.shouldReplicate(string(event.Table.Table)) {
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
			// TODO save the GTID (or file:position if we don't have GTID) for recovery
			err := r.tx.Commit()
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

func (r *Replicator) shouldReplicate(table string) bool {
	if len(r.config.Config.Tables) == 0 {
		// No tables configged, we replicate everything
		return true
	}
	_, exists := r.config.Config.Tables[table]
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
