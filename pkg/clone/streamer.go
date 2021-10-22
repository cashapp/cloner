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

// stream converts raw rows to a stream
func stream(table *Table, raws [][]interface{}) RowStream {
	rows := make([]*Row, len(raws))
	for i, raw := range raws {
		rows[i] = table.ToRow(raw)
	}
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

func StreamChunk(ctx context.Context, conn DBReader, chunk Chunk, hint string, extraWhereClause string) (RowStream, error) {
	table := chunk.Table
	columns := table.ColumnList

	where, params := chunkWhere(chunk, extraWhereClause)
	stmt := fmt.Sprintf("select %s %s from %s %s order by %s asc", columns, hint, table.Name, where, table.IDColumn)
	rows, err := conn.QueryContext(ctx, stmt, params...)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return newRowStream(table, rows)
}

func chunkWhere(chunk Chunk, extraWhereClause string) (string, []interface{}) {
	table := chunk.Table

	var clauses []string
	var params []interface{}
	if extraWhereClause != "" {
		clauses = append(clauses, "("+extraWhereClause+")")
	}

	if len(table.ChunkColumns) == 1 {
		// If we only have a single column then life is so simple...
		clauses = append(clauses,
			fmt.Sprintf("%s >= ?", table.ChunkColumns[0]),
			fmt.Sprintf("%s < ?", table.ChunkColumns[0]))
		params = append(params, chunk.Start[0], chunk.End[0])

	} else {
		// If we have more than a single column then things gets hairy
		// First we compare by each column separately to maximize index efficiency
		// but for this to work the comparison on the end will be closed rather than open
		// which means we include more rows than the chunk might contain if you look like a sequence like this
		// (1,1) (1,2) (chunk ends here) (1,3) (1,4)
		// In order to not also select the last two rows we need to compare by a concatenation of the columns
		// This would be enough but it seems very unlikely that a query planner would be clever enough to
		// be able to use indexes for that

		// First each columns separately
		for i, column := range table.ChunkColumns {
			clauses = append(clauses,
				fmt.Sprintf("%s >= ?", column),
				fmt.Sprintf("%s <= ?", column))
			params = append(params, chunk.Start[i], chunk.End[i])
		}

		// Then the concatenation of columns
		var questionMarks strings.Builder
		var columns strings.Builder
		for i, column := range table.ChunkColumns {
			questionMarks.WriteString("?")
			columns.WriteString("`")
			columns.WriteString(column)
			columns.WriteString("`")
			if i < len(table.ChunkColumns)-1 {
				questionMarks.WriteString(", ")
				columns.WriteString(", ")
			}
		}

		clauses = append(clauses,
			fmt.Sprintf("(%s) >= (%s)", columns.String(), questionMarks.String()))
		params = append(params, chunk.Start...)

		clauses = append(clauses,
			fmt.Sprintf("(%s) < (%s)", columns.String(), questionMarks.String()))
		params = append(params, chunk.End...)
	}

	if len(clauses) == 0 {
		return "", []interface{}{}
	} else {
		return "where " + strings.Join(clauses, " and "), params
	}
}
