package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	"strings"
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

// PkAfterOrEqual returns true if the pk of the row is higher or equal to the PK of the receiver row
func (r *Row) PkAfterOrEqual(row []interface{}) bool {
	return r.ID >= r.Table.PkOfRow(row)
}

// PkEqual returns true if the pk of the row is equal to the PK of the receiver row
func (r *Row) PkEqual(row []interface{}) bool {
	return r.ID == r.Table.PkOfRow(row)
}

func (r *Row) Updated(row []interface{}) *Row {
	if r.Table.PkOfRow(row) != r.ID {
		panic("updating row with another ID")
	}
	return &Row{
		Table: r.Table,
		ID:    r.ID,
		Data:  row,
	}
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
	rows, err := readAll(stream)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return &bufferStream{rows}, nil
}

// stream converts buffered rows to a stream
func stream(rows []*Row) RowStream {
	return &bufferStream{rows}
}

// buffer buffers all of the rows into memory
func readAll(stream RowStream) ([]*Row, error) {
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
	return rows, nil
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

func StreamChunk2(ctx context.Context, conn DBReader, chunk Chunk2, hint string, extraWhereClause string) (RowStream, error) {
	table := chunk.Table
	columns := table.ColumnList

	where := chunk2Where(chunk, extraWhereClause)
	stmt := fmt.Sprintf("select %s %s from %s %s order by %s asc", columns, hint, table.Name, where, table.IDColumn)
	rows, err := conn.QueryContext(ctx, stmt)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return newRowStream(table, rows)
}

func chunk2Where(chunk Chunk2, extraWhereClause string) string {
	table := chunk.Table
	var clauses []string
	if extraWhereClause != "" {
		clauses = append(clauses, "("+extraWhereClause+")")
	}
	clauses = append(clauses,
		fmt.Sprintf("%s >= %d", table.IDColumn, chunk.Start),
		fmt.Sprintf("%s < %d", table.IDColumn, chunk.End))
	if len(clauses) == 0 {
		return ""
	} else {
		return "where " + strings.Join(clauses, " and ")
	}
}
