package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// DBReader is an interface that can be implemented by sql.Conn or sql.Tx or sql.DB so that we can
// easily change synchronization method
type DBReader interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type Row struct {
	Table *Table
	ID    int64
	Data  []interface{}
}

type bufferStream struct {
	rows []*Row
}

func (b *bufferStream) Next() (*Row, error) {
	if len(b.rows) == 0 {
		return nil, nil
	}
	row := b.rows[0]
	b.rows = b.rows[1:]
	return row, nil
}

func (b *bufferStream) Close() error {
	// nothing to do here
	return nil
}

// buffer buffers all of the rows into memory
func buffer(stream RowStream) (RowStream, error) {
	defer stream.Close()
	var rows []*Row
	for {
		row, err := stream.Next()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if row == nil {
			break
		}
		rows = append(rows, row)
	}
	return &bufferStream{rows}, nil
}

type RowStream interface {
	// Next returns the next row or nil if we're done
	Next() (*Row, error)
	// Close releases any potential underlying resources
	Close() error
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

func (s *rowStream) Next() (*Row, error) {
	if !s.rows.Next() {
		return nil, nil
	}
	cols, err := s.rows.Columns()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	row := make([]interface{}, len(cols))

	var id int64

	scanArgs := make([]interface{}, len(row))
	for i := range row {
		if i == s.table.IDColumnIndex {
			scanArgs[i] = &id
		} else {
			scanArgs[i] = &row[i]
		}
	}
	err = s.rows.Scan(scanArgs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// We replaced the data in the row slice with pointers to the local vars, so lets put this back after the read
	row[s.table.IDColumnIndex] = id
	return &Row{
		Table: s.table,
		ID:    id,
		Data:  row,
	}, nil
}

func (s *rowStream) Close() error {
	return s.rows.Close()
}

func StreamChunk(ctx context.Context, conn DBReader, chunk Chunk, extraWhereClause string) (RowStream, error) {
	table := chunk.Table
	columns := table.ColumnList

	logger := log.WithField("table", chunk.Table.Name)
	if chunk.First && chunk.Last {
		logger.Debugf("reading chunk -")
		where := ""
		if extraWhereClause != "" {
			where = fmt.Sprintf(" where %s", extraWhereClause)
		}
		rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %s from %s%s order by %s asc",
			columns, table.Name, where, table.IDColumn))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return newRowStream(table, rows)
	} else {
		where := ""
		if extraWhereClause != "" {
			where = fmt.Sprintf(" %s and", extraWhereClause)
		}
		if chunk.First {
			logger.Debugf("reading chunk -%v", chunk.End)
			rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %s from %s where%s %s < ? order by %s asc",
				columns, table.Name, where, table.IDColumn, table.IDColumn), chunk.End)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return newRowStream(table, rows)
		} else if chunk.Last {
			logger.Debugf("reading chunk %v-", chunk.Start)
			rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %s from %s where%s %s >= ? order by %s asc",
				columns, table.Name, where, table.IDColumn, table.IDColumn), chunk.Start)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return newRowStream(table, rows)
		} else {
			logger.Debugf("reading chunk [%v-%v)", chunk.Start, chunk.End)
			rows, err := conn.QueryContext(ctx,
				fmt.Sprintf("select %s from %s where%s %s >= ? and %s < ? order by %s asc",
					columns, table.Name, where, table.IDColumn, table.IDColumn, table.IDColumn), chunk.Start, chunk.End)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			return newRowStream(table, rows)
		}
	}
}
