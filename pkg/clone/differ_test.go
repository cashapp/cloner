package clone

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testRow struct {
	id    int64
	table string
	data  string
}

func (testRow testRow) toRow(batchSize int) *Row {
	return &Row{
		Table: &Table{
			Name:             testRow.table,
			KeyColumns:       []string{"id"},
			KeyColumnIndexes: []int{0},
			Config:           TableConfig{WriteBatchSize: batchSize},
		},
		Data: []interface{}{testRow.id, testRow.data},
	}
}

type testDiff struct {
	diffType MutationType
	row      testRow
}

func (d testDiff) toDiff(batchSize int) Diff {
	return Diff{d.diffType, d.row.toRow(batchSize), nil}
}

func toTestRow(row *Row) testRow {
	return testRow{row.KeyValues()[0].(int64), row.Table.Name, row.Data[1].(string)}
}

func DisableTestStreamDiff(t *testing.T) {
	customers := &Table{
		Name:             "customers",
		KeyColumns:       []string{"id"},
		KeyColumnIndexes: []int{0},
		Config:           TableConfig{WriteBatchSize: 5},
	}
	transactions := &Table{
		Name:             "transactions",
		KeyColumns:       []string{"customer_id", "id"},
		KeyColumnIndexes: []int{0, 1},
		Config:           TableConfig{WriteBatchSize: 5},
	}
	tests := []struct {
		name   string
		table  *Table
		source []*Row
		target []*Row
		diff   []Diff
	}{
		{
			name:   "empty",
			table:  customers,
			source: nil,
			target: nil,
			diff:   nil,
		},
		{
			name:  "same",
			table: customers,
			source: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
			},
			target: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
			},
			diff: nil,
		},
		{
			name:  "1 new",
			table: customers,
			source: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
			},
			target: nil,
			diff:   []Diff{{Insert, &Row{Table: customers, Data: []interface{}{1, "A"}}, nil}},
		},
		{
			name:   "1 deleted",
			table:  customers,
			source: nil,
			target: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
			},
			diff: []Diff{{Delete, &Row{Table: customers, Data: []interface{}{1, "A"}}, nil}},
		},
		{
			name:  "1 updated",
			table: customers,
			source: []*Row{
				{Table: customers, Data: []interface{}{1, "B"}},
			},
			target: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
			},
			diff: []Diff{{Update,
				&Row{Table: customers, Data: []interface{}{1, "B"}},
				&Row{Table: customers, Data: []interface{}{1, "A"}}}},
		},
		{
			name:  "1 same 1 inserted",
			table: customers,
			source: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{2, "B"}},
			},
			target: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
			},
			diff: []Diff{{Insert, &Row{Table: customers, Data: []interface{}{2, "B"}}, nil}},
		},
		{
			name:  "1 same 1 deleted",
			table: customers,
			source: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
			},
			target: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{2, "B"}},
			},
			diff: []Diff{{Delete, &Row{Table: customers, Data: []interface{}{2, "B"}}, nil}},
		},
		{
			name:  "1 same 1 updated",
			table: customers,
			source: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{2, "B"}},
			},
			target: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{2, "A"}},
			},
			diff: []Diff{{Update, &Row{Table: customers, Data: []interface{}{2, "B"}}, &Row{Table: customers, Data: []interface{}{2, "A"}}}},
		},
		{
			name:  "1 inserted middle",
			table: customers,
			source: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{2, "B"}},
				{Table: customers, Data: []interface{}{3, "C"}},
			},
			target: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{3, "C"}},
			},
			diff: []Diff{
				{Insert, &Row{Table: customers, Data: []interface{}{2, "B"}}, nil},
			},
		},
		{
			name:  "2 inserted middle",
			table: customers,
			source: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{2, "B"}},
				{Table: customers, Data: []interface{}{3, "C"}},
				{Table: customers, Data: []interface{}{4, "D"}},
			},
			target: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{4, "D"}},
			},
			diff: []Diff{
				{Insert, &Row{Table: customers, Data: []interface{}{2, "B"}}, nil},
				{Insert, &Row{Table: customers, Data: []interface{}{3, "C"}}, nil},
			},
		},
		{
			name:  "2 deleted middle",
			table: customers,
			source: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{4, "D"}},
			},
			target: []*Row{
				{Table: customers, Data: []interface{}{1, "A"}},
				{Table: customers, Data: []interface{}{2, "B"}},
				{Table: customers, Data: []interface{}{3, "C"}},
				{Table: customers, Data: []interface{}{4, "D"}},
			},
			diff: []Diff{
				{Delete, &Row{Table: customers, Data: []interface{}{2, "B"}}, nil},
				{Delete, &Row{Table: customers, Data: []interface{}{3, "C"}}, nil},
			},
		},
		{
			name:  "multi column 1 same 1 deleted",
			table: transactions,
			source: []*Row{
				{Table: transactions, Data: []interface{}{1, 1, "A"}},
			},
			target: []*Row{
				{Table: transactions, Data: []interface{}{1, 1, "A"}},
				{Table: transactions, Data: []interface{}{2, 1, "B"}},
			},
			diff: []Diff{{Delete, &Row{Table: transactions, Data: []interface{}{2, 1, "B"}}, nil}},
		},
		{
			name:  "multi column 1 same 1 updated",
			table: transactions,
			source: []*Row{
				{Table: transactions, Data: []interface{}{1, 1, "A"}},
				{Table: transactions, Data: []interface{}{2, 1, "B"}},
			},
			target: []*Row{
				{Table: transactions, Data: []interface{}{1, 1, "A"}},
				{Table: transactions, Data: []interface{}{2, 1, "A"}},
			},
			diff: []Diff{{Update,
				&Row{Table: transactions, Data: []interface{}{2, 1, "B"}},
				&Row{Table: transactions, Data: []interface{}{2, 1, "A"}}}},
		},
		{
			name:  "multi column new rows lower id",
			table: transactions,
			source: []*Row{
				{Table: transactions, Data: []interface{}{1, 4, "A"}},
				{Table: transactions, Data: []interface{}{2, 3, "B"}},
				{Table: transactions, Data: []interface{}{3, 2, "C"}},
				{Table: transactions, Data: []interface{}{4, 1, "D"}},
			},
			target: []*Row{
				{Table: transactions, Data: []interface{}{1, 4, "A"}},
				{Table: transactions, Data: []interface{}{4, 1, "D"}},
			},
			diff: []Diff{
				{Insert, &Row{Table: transactions, Data: []interface{}{2, 3, "B"}}, nil},
				{Insert, &Row{Table: transactions, Data: []interface{}{3, 2, "C"}}, nil},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diffs, err := StreamDiff(context.Background(), test.table,
				streamRows(test.source),
				streamRows(test.target))
			assert.NoError(t, err)
			assert.Equal(t, test.diff, diffs)
		})
	}
}

func streamRows(rows []*Row) RowStream {
	return &bufferStream{rows}
}

func DisableTestRowsEqual(t *testing.T) {
	sourceRow := &Row{nil, []interface{}{
		100020406,
		int64(30027935561),
		[]byte{51, 48, 48, 50, 55, 57, 51, 53, 53, 54, 49},
	}}
	targetRow := &Row{nil, []interface{}{
		100020406,
		[]byte{51, 48, 48, 50, 55, 57, 51, 53, 53, 54, 49},
		[]byte{51, 48, 48, 50, 55, 57, 51, 53, 53, 54, 49},
	}}
	isEqual, err := RowsEqual(
		sourceRow,
		targetRow,
	)
	assert.NoError(t, err)
	assert.True(t, isEqual)
}
