package clone

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"reflect"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/topodata"
)

type Row struct {
	Table *Table
	ID    int64
	Data  []interface{}
}

type RowStream interface {
	// Next returns the next row or nil if we're done
	Next(context.Context) (*Row, error)
}

type rowStream struct {
	table   *Table
	rows    *sql.Rows
	columns []string
}

func newRowStream(table *Table, rows *sql.Rows) (*rowStream, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &rowStream{table, rows, columns}, nil
}

type rejectStream struct {
	source RowStream
	reject func(row *Row) (bool, error)
}

func (f *rejectStream) Next(ctx context.Context) (*Row, error) {
	for {
		next, err := f.source.Next(ctx)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if next == nil {
			return next, nil
		}
		reject, err := f.reject(next)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if !reject {
			return next, nil
		}
	}
}

func newRejectStream(stream RowStream, filter func(row *Row) (bool, error)) RowStream {
	return &rejectStream{stream, filter}
}

func filterStreamByShard(stream RowStream, table *Table, targetFilter []*topodata.KeyRange) RowStream {
	return newRejectStream(stream, func(row *Row) (bool, error) {
		shardingValue, err := toUint64(row.Data[table.ShardingColumnIndex])
		if err != nil {
			return false, errors.Wrapf(err, "could not read column %s.%s", table.Name, table.ShardingColumn)
		}
		return !InShard(shardingValue, targetFilter), nil
	})
}

func toUint64(val interface{}) (uint64, error) {
	// TODO there's gotta be a better way to do this...
	maybeUint64, ok := val.(*uint64)
	if ok {
		return *maybeUint64, nil
	}
	maybePInt64, ok := val.(*int64)
	if ok {
		return uint64(*maybePInt64), nil
	}
	maybeInt64, ok := val.(int64)
	if ok {
		return uint64(maybeInt64), nil
	}
	maybeBytes, ok := val.([]byte)
	if ok {
		// mysql uses little endian
		return binary.LittleEndian.Uint64(maybeBytes), nil
	}
	maybePointer, ok := val.(*interface{})
	if ok {
		return toUint64(*maybePointer)
	}
	return 0, errors.Errorf("Could not convert to uint64: %v", reflect.TypeOf(val))
}

func InShard(id uint64, shard []*topodata.KeyRange) bool {
	destination := key.DestinationKeyspaceID(vhash(id))
	for _, keyRange := range shard {
		if key.KeyRangeContains(keyRange, destination) {
			return true
		}
	}
	return false
}

func (r *rowStream) Next(ctx context.Context) (*Row, error) {
	if !r.rows.Next() {
		return nil, nil
	}
	cols, err := r.rows.Columns()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var id int64
	row := make([]interface{}, len(cols))
	for i, _ := range row {
		if r.table.IDColumnIndex == i {
			row[i] = &id
		} else {
			row[i] = new(interface{})
		}
	}
	err = r.rows.Scan(row...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &Row{
		Table: r.table,
		ID:    id,
		Data:  row,
	}, nil
}

func StreamChunk(ctx context.Context, conn *sql.Conn, chunk Chunk) (RowStream, error) {
	table := chunk.Table
	columns := table.ColumnList

	if chunk.First {
		log.Debugf("Reading chunk -%v", chunk.End)
		rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %s from %s where %s <= ?",
			columns, table.Name, table.IDColumn), chunk.End)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newRowStream(table, rows)
	} else if chunk.Last {
		log.Debugf("Reading chunk %v-", chunk.Start)
		rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %s from %s where %s >= ?",
			columns, table.Name, table.IDColumn), chunk.Start)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newRowStream(table, rows)
	} else {
		log.Debugf("Reading chunk %v-%v", chunk.Start, chunk.End)
		rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %s from %s where %s >= ? and %s <= ?",
			columns, table.Name, table.IDColumn, table.IDColumn), chunk.Start, chunk.End)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newRowStream(table, rows)
	}
}
