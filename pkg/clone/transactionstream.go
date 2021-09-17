package clone

import (
	"context"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"github.com/pkg/errors"
	_ "net/http/pprof"
)

type Mutation struct {
	Type  MutationType
	Table *Table
	Rows  [][]interface{}
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

func NewTransactionStreamer(config Replicate, tables []*Table) (*TransactionStream, error) {
	var err error
	r := TransactionStream{
		config:      config,
		tables:      tables,
		schemaCache: make(map[uint64]*Table),
	}
	r.syncerCfg, err = r.config.Source.BinlogSyncerConfig(r.config.ServerID)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.sourceSchema, err = r.config.Source.Schema()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &r, nil
}

func (r *TransactionStream) Run(ctx context.Context, b backoff.BackOff, position Position, output chan Transaction) error {
	var err error

	syncer := replication.NewBinlogSyncer(r.syncerCfg)
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

		eventsReceived.Inc()

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
			if !r.shouldReplicate(event.Table) {
				ignored = true
				continue
			}
			currentTransaction.Mutations = append(currentTransaction.Mutations, r.toMutation(e, event))
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
			eventsIgnored.Inc()
		} else {
			eventsProcessed.Inc()
		}
	}
}

func (r *TransactionStream) toMutation(e *replication.BinlogEvent, event *replication.RowsEvent) Mutation {
	return Mutation{
		Type:  toMutationType(e.Header.EventType),
		Table: r.getTableSchema(event.Table),
		Rows:  event.Rows,
	}
}

func (r *TransactionStream) getTableSchema(event *replication.TableMapEvent) *Table {
	tableSchema, ok := r.schemaCache[event.TableID]
	if !ok {
		for _, table := range r.tables {
			if table.Name == string(event.Table) {
				r.schemaCache[event.TableID] = table
				return table
			}
		}
		r.schemaCache[event.TableID] = nil
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

func (r *TransactionStream) shouldReplicate(event *replication.TableMapEvent) bool {
	if r.sourceSchema != string(event.Schema) {
		return false
	}
	return r.getTableSchema(event) != nil
}
