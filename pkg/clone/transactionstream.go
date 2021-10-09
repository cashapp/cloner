package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"hash/fnv"
	_ "net/http/pprof"
)

type Mutation struct {
	Type  MutationType
	Table *Table
	Rows  [][]interface{}

	// Chunk is only sent with a Repair mutation type
	Chunk Chunk
}

type Transaction struct {
	Mutations     []Mutation
	FinalPosition Position
}

type Position struct {
	File     string
	Position uint32
	// Gset can be nil in case database does not support GTID
	Gset mysql.GTIDSet
}

// TransactionStream consumes binlog events and emits full transactions
type TransactionStream struct {
	config       Replicate
	syncerCfg    replication.BinlogSyncerConfig
	sourceSchema string
	tables       []*Table

	schemaCache map[uint64]*Table
}

func NewTransactionStreamer(config Replicate) (*TransactionStream, error) {
	r := TransactionStream{
		config:      config,
		schemaCache: make(map[uint64]*Table),
	}
	return &r, nil
}

func (s *TransactionStream) Run(ctx context.Context, b backoff.BackOff, output chan Transaction) error {
	var err error

	position, err := s.readStartingPosition(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	syncer := replication.NewBinlogSyncer(s.syncerCfg)
	defer syncer.Close()

	var streamer *replication.BinlogStreamer
	if position.Gset != nil {
		streamer, err = syncer.StartSyncGTID(position.Gset)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		streamer, err = syncer.StartSync(mysql.Position{Pos: position.Position, Name: position.File})
		if err != nil {
			return errors.WithStack(err)
		}
	}

	var nextPos mysql.Position
	var currentTransaction *Transaction

	for {
		e, err := streamer.GetEvent(ctx)
		if err != nil {
			return errors.WithStack(err)
		}

		eventsReceived.WithLabelValues(s.config.TaskName).Inc()

		if e.Header.LogPos > 0 {
			// Some events like FormatDescriptionEvent return 0, ignore.
			nextPos.Pos = e.Header.LogPos
		}

		// TODO we should crash hard if we get a DDL as we currently do not support that

		ignored := false
		switch event := e.Event.(type) {
		case *replication.RotateEvent:
			nextPos.Name = string(event.NextLogName)
			nextPos.Pos = uint32(event.Position)
		case *replication.QueryEvent:
			if string(event.Query) == "BEGIN" {
				currentTransaction = &Transaction{}
			} else {
				ignored = true
			}
		case *replication.RowsEvent:
			if !s.shouldReplicate(event.Table) {
				ignored = true
				continue
			}
			currentTransaction.Mutations = append(currentTransaction.Mutations, s.toMutation(e, event))
		case *replication.XIDEvent:
			gset := event.GSet
			currentTransaction.FinalPosition = Position{
				File:     nextPos.Name,
				Position: nextPos.Pos,
				Gset:     gset,
			}
			select {
			case output <- *currentTransaction:
			case <-ctx.Done():
				return ctx.Err()
			}
			currentTransaction = nil
			// We've received a full transaction, we can reset the backoff
			b.Reset()
		default:
			ignored = true
		}

		if ignored {
			eventsIgnored.WithLabelValues(s.config.TaskName).Inc()
		} else {
			eventsProcessed.WithLabelValues(s.config.TaskName).Inc()
		}
	}
}

func (s *TransactionStream) toMutation(e *replication.BinlogEvent, event *replication.RowsEvent) Mutation {
	return Mutation{
		Type:  toMutationType(e.Header.EventType),
		Table: s.getTableSchema(event.Table),
		Rows:  event.Rows,
	}
}

func (s *TransactionStream) getTableSchema(event *replication.TableMapEvent) *Table {
	tableSchema, ok := s.schemaCache[event.TableID]
	if !ok {
		for _, table := range s.tables {
			if table.Name == string(event.Table) {
				s.schemaCache[event.TableID] = table
				return table
			}
		}
		s.schemaCache[event.TableID] = nil
		return nil
	}
	return tableSchema
}

func toMutationType(eventType replication.EventType) MutationType {
	switch eventType {
	case replication.DELETE_ROWS_EVENTv0:
	case replication.DELETE_ROWS_EVENTv1:
	case replication.DELETE_ROWS_EVENTv2:
		return Delete
	case replication.WRITE_ROWS_EVENTv0:
	case replication.WRITE_ROWS_EVENTv1:
	case replication.WRITE_ROWS_EVENTv2:
		return Insert
	case replication.UPDATE_ROWS_EVENTv0:
	case replication.UPDATE_ROWS_EVENTv1:
	case replication.UPDATE_ROWS_EVENTv2:
		return Update
	default:
		panic(fmt.Sprintf("unsupported row event type: %s", eventType.String()))
	}
	return 0
}

func (s *TransactionStream) shouldReplicate(event *replication.TableMapEvent) bool {
	if s.sourceSchema != string(event.Schema) {
		return false
	}
	return s.getTableSchema(event) != nil
}

func (s *TransactionStream) Init(ctx context.Context) error {
	var err error

	if s.config.ServerID == 0 {
		hasher := fnv.New32()
		_, err = hasher.Write([]byte(s.config.TaskName))
		if err != nil {
			return errors.WithStack(err)
		}
		s.config.ServerID = hasher.Sum32()
	}
	logrus.Infof("using replication server id: %d", s.config.ServerID)

	s.syncerCfg, err = s.config.Source.BinlogSyncerConfig(s.config.ServerID)
	if err != nil {
		return errors.WithStack(err)
	}
	s.sourceSchema, err = s.config.Source.Schema()
	if err != nil {
		return errors.WithStack(err)
	}

	s.tables, err = LoadTables(ctx, s.config.ReaderConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	source, err := s.config.Source.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer source.Close()

	// TODO adding this table to the list of tables to replicate should be moved to the Heartbeater
	heartbeatTable, err := loadTable(ctx, s.config.ReaderConfig, s.config.Source.Type, source, s.sourceSchema, s.config.HeartbeatTable, TableConfig{})
	if err != nil {
		return errors.WithStack(err)
	}
	s.tables = append(s.tables, heartbeatTable)

	// TODO adding this table to the list of tables to replicate should be moved to the Snapshotter
	watermarkTable, err := loadTable(ctx, s.config.ReaderConfig, s.config.Source.Type, source, s.sourceSchema, s.config.WatermarkTable, TableConfig{})
	if err != nil {
		return errors.WithStack(err)
	}
	s.tables = append(s.tables, watermarkTable)

	return nil
}

func (s *TransactionStream) readStartingPosition(ctx context.Context) (Position, error) {
	logger := logrus.WithContext(ctx).WithField("task", "replicate")

	file, position, executedGtidSet, err := s.readCheckpoint(ctx)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			file, position, executedGtidSet, err = s.readMasterPosition(ctx)
			logger.Infof("starting new replication from current master position %s:%d gtid=%s", file, position, executedGtidSet)
			if err != nil {
				return Position{}, errors.WithStack(err)
			}
		} else {
			return Position{}, errors.WithStack(err)
		}
	} else {
		masterFile, masterPos, masterGtidSet, err := s.readMasterPosition(ctx)
		if err != nil {
			return Position{}, errors.WithStack(err)
		}
		logger.Infof("re-starting replication from %s:%d gtid=%s (master is currently at %s:%d gtid=%s)",
			file, position, executedGtidSet, masterFile, masterPos, masterGtidSet)
	}

	// We sometimes have a GTIDSet, if not we return nil
	var gset mysql.GTIDSet
	if executedGtidSet != "" {
		parsed, err := mysql.ParseGTIDSet(s.syncerCfg.Flavor, executedGtidSet)
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

func (s *TransactionStream) readMasterPosition(ctx context.Context) (file string, position uint32, executedGtidSet string, err error) {
	source, err := s.config.Source.DB()
	if err != nil {
		return
	}
	defer source.Close()

	row := source.QueryRowContext(ctx, "SHOW MASTER STATUS")
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

func (s *TransactionStream) readCheckpoint(ctx context.Context) (file string, position uint32, executedGtidSet string, err error) {
	target, err := s.config.Target.DB()
	if err != nil {
		return
	}
	defer target.Close()

	row := target.QueryRowContext(ctx,
		fmt.Sprintf("SELECT file, position, gtid_set FROM %s WHERE name = ?",
			s.config.CheckpointTable),
		s.config.TaskName)
	err = row.Scan(
		&file,
		&position,
		&executedGtidSet,
	)
	return
}
