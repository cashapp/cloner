package clone

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"unsafe"

	"github.com/pkg/errors"
)

// DBReader is an interface that can be implemented by sql.Conn or sql.Tx or sql.DB so that we can
// easily change synchronization method
type DBReader interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type Row struct {
	Table *Table
	Data  []interface{}
}

// PkAfterOrEqual returns true if the pk of the row is higher or equal to the PK of the receiver row
func (r *Row) PkAfterOrEqual(row []interface{}) bool {
	return genericCompareKeys(r.KeyValues(), r.Table.KeysOfRow(row)) >= 0
}

// PkEqual returns true if the pk of the row is equal to the PK of the receiver row
func (r *Row) PkEqual(row []interface{}) bool {
	return genericCompareKeys(r.KeyValues(), r.Table.KeysOfRow(row)) == 0
}

func (r *Row) Updated(row []interface{}) *Row {
	if genericCompareKeys(r.KeyValues(), r.Table.KeysOfRow(row)) != 0 {
		panic("updating row with another ID")
	}
	return &Row{
		Table: r.Table,
		Data:  row,
	}
}

func (r *Row) KeyValues() []interface{} {
	if len(r.Table.KeyColumns) == 0 {
		panic("need key columns")
	}
	values := make([]interface{}, len(r.Table.KeyColumns))
	for i, index := range r.Table.KeyColumnIndexes {
		values[i] = r.Data[index]
	}
	return values
}

func (r *Row) AppendKeyValues(values []interface{}) []interface{} {
	for _, i := range r.Table.KeyColumnIndexes {
		values = append(values, r.Data[i])
	}
	return values
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

func (b *bufferStream) SizeBytes() (size uint64) {
	size = 0
	for _, row := range b.rows {
		size += uint64(unsafe.Sizeof(row.Data))
	}
	return
}

// buffer buffers all of the rows into memory
func buffer(stream RowStream) (*bufferStream, error) {
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

	scanArgs := make([]interface{}, len(row))
	for i := range row {
		scanArgs[i] = &row[i]
	}
	err = s.rows.Scan(scanArgs...)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// We replaced the data in the row slice with pointers to the local vars, so lets put this back after the read
	return &Row{
		Table: s.table,
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
	stmt := fmt.Sprintf("select %s %s from %s %s order by %s", columns, hint, table.Name, where,
		table.KeyColumnList)
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
		clauses = append(clauses, extraWhereClause)
	}

	// Expanding the row constructor comparisons
	if chunk.Start != nil {
		c, p := expandRowConstructorComparison(table.KeyColumns, ">=", chunk.Start)
		clauses = append(clauses, c)
		params = append(params, p...)
	}

	if chunk.End != nil {
		c, p := expandRowConstructorComparison(table.KeyColumns, "<", chunk.End)
		clauses = append(clauses, c)
		params = append(params, p...)
	}

	if len(clauses) == 0 {
		return "", []interface{}{}
	} else {
		return "where (" + strings.Join(clauses, ") and (") + ")", params
	}
}

// expandRowConstructorComparison expands a row constructor comparison to make sure we use indexes properly,
//
//	see this link for more detail: https://dev.mysql.com/doc/refman/5.7/en/row-constructor-optimization.html
//	more recent versions of mysql might handle this better but TiDB doesn't yet:
//	https://github.com/pingcap/tidb/issues/28789
func expandRowConstructorComparison(left []string, operator string, right []interface{}) (string, []interface{}) {
	if len(left) != len(right) {
		panic("left hand should be same size as right hand")
	}

	if len(left) == 1 {
		return fmt.Sprintf("`%s` %s ?", left[0], operator), right
	}
	if len(left) > 2 {
		// TODO I'm just too tired to figure out how to expand this with more than two columns
		panic("currently only support two operands")
	}
	if operator == "=" {
		return fmt.Sprintf("`%s` = ? and `%s` = ?", left[0], left[1]), right
	}
	parentOperator := operator
	switch operator {
	case ">=":
		parentOperator = ">"
	case "<=":
		parentOperator = "<"
	}
	// a > ? or (a = ? and b > ?)
	return fmt.Sprintf("`%s` %s ? or (`%s` = ? and `%s` %s ?)",
			left[0], parentOperator, left[0], left[1], operator),
		[]interface{}{right[0], right[0], right[1]}
}
