package clone

import (
	"testing"

	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOngoingChunkReconcileBinlogEvents(t *testing.T) {
	tests := []struct {
		name         string
		chunk        testChunk
		start        int64
		end          int64
		eventType    MutationType
		startingRows [][]interface{}
		beforeRows   [][]interface{}
		eventRows    [][]interface{}
		resultRows   [][]interface{}
	}{
		{
			name:  "delete outside chunk range",
			start: 5,  // inclusive
			end:   11, // exclusive
			startingRows: [][]interface{}{
				{5, "customer name"},
			},

			eventType: Delete,
			eventRows: [][]interface{}{
				{11, "other customer name"},
			},

			resultRows: [][]interface{}{
				{5, "customer name"},
			},
		},
		{
			name:  "delete inside chunk range",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{5, "customer name"},
			},

			eventType: Delete,
			eventRows: [][]interface{}{
				{5, "customer name"},
			},

			resultRows: [][]interface{}{},
		},
		{
			name:  "insert after",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{5, "customer name"},
			},

			eventType: Insert,
			eventRows: [][]interface{}{
				{6, "other customer name"},
			},

			resultRows: [][]interface{}{
				{5, "customer name"},
				{6, "other customer name"},
			},
		},
		{
			name:  "insert before",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{6, "customer name"},
			},

			eventType: Insert,
			eventRows: [][]interface{}{
				{5, "other customer name"},
			},

			resultRows: [][]interface{}{
				{5, "other customer name"},
				{6, "customer name"},
			},
		},
		{
			name:  "insert middle",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{5, "customer name #5"},
				{7, "customer name #7"},
			},

			eventType: Insert,
			eventRows: [][]interface{}{
				{6, "customer name #6"},
			},

			resultRows: [][]interface{}{
				{5, "customer name #5"},
				{6, "customer name #6"},
				{7, "customer name #7"},
			},
		},
		{
			name:  "multiple rows mixed insert and update",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{5, "customer name #5"},
				{7, "customer name #7"},
			},

			eventType: Insert,
			eventRows: [][]interface{}{
				{6, "customer name #6"},
				{7, "customer name #7 updated"},
			},

			resultRows: [][]interface{}{
				{5, "customer name #5"},
				{6, "customer name #6"},
				{7, "customer name #7 updated"},
			},
		},
		{
			name:  "update",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{6, "customer name"},
				{7, "other customer name"},
			},

			eventType: Update,
			beforeRows: [][]interface{}{
				{6, "customer name"},
				{11, "outside of chunk range"},
			},
			eventRows: [][]interface{}{
				{6, "updated customer name"},
				{11, "updated outside of chunk range"},
			},

			resultRows: [][]interface{}{
				{6, "updated customer name"},
				{7, "other customer name"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableName := "customer"
			tableSchema := &schema.Table{
				Name:      tableName,
				PKColumns: []int{0},
				Columns: []schema.TableColumn{
					{Name: "id"},
					{Name: "name"},
				},
			}
			table := &Table{
				Name:             tableName,
				MysqlTable:       tableSchema,
				KeyColumns:       []string{"id"},
				KeyColumnIndexes: []int{0},
			}
			inputRows := make([]*Row, len(test.startingRows))
			for i, row := range test.startingRows {
				inputRows[i] = &Row{
					Table: table,
					Data:  row,
				}
			}
			chunk := &ChunkSnapshot{
				InsideWatermarks: true,
				Rows:             inputRows,
				Chunk: Chunk{
					Start: []interface{}{test.start},
					End:   []interface{}{test.end},
					Table: table,
				},
			}
			_, err := chunk.reconcileBinlogEvent(
				Mutation{
					Type:   test.eventType,
					Table:  table,
					Before: test.beforeRows,
					Rows:   test.eventRows,
				},
			)
			require.NoError(t, err)

			assert.Equal(t, len(test.resultRows), len(chunk.Rows))
			for i, expectedRow := range test.resultRows {
				actualRow := chunk.Rows[i]
				for j, cell := range expectedRow {
					assert.Equal(t, cell, actualRow.Data[j])
				}
			}
		})
	}
}

func TestChunkSortAndFind(t *testing.T) {
	table := &Table{
		KeyColumnIndexes: []int{0, 1, 2},
	}
	c := &ChunkSnapshot{
		Rows: []*Row{
			{
				Table: table,
				Data:  []interface{}{2, 1, 2, 0, "row #3"},
			},
			{
				Table: table,
				Data:  []interface{}{1, 1, 2, 0, "row #2"},
			},
			{
				Table: table,
				Data:  []interface{}{0, 0, 1, 0, "row #1"},
			},
		},
		Chunk: Chunk{
			Table: table,
		},
	}
	c.sort()
	// First row
	row, i, err := c.findRow([]interface{}{0, 0, 1})
	assert.NoError(t, err)
	assert.NotNil(t, row)
	assert.Equal(t, "row #1", row.Data[4])
	assert.Equal(t, 0, i)
	// Middle row
	row, i, err = c.findRow([]interface{}{1, 1, 2})
	assert.NoError(t, err)
	assert.NotNil(t, row)
	assert.Equal(t, "row #2", row.Data[4])
	assert.Equal(t, 1, i)
	// Non-existing row between rows 2 and 3
	row, i, err = c.findRow([]interface{}{2, 1, 1})
	assert.NoError(t, err)
	assert.Nil(t, row)
	assert.Equal(t, 2, i)
	// Last row
	row, i, err = c.findRow([]interface{}{2, 1, 2, 0, 0})
	assert.NoError(t, err)
	assert.NotNil(t, row)
	assert.Equal(t, "row #3", row.Data[4])
	assert.Equal(t, 2, i)
}
