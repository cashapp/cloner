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
			eventRows: [][]interface{}{
				{6, "updated customer name"},
				{11, "outside of chunk range"},
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
			err := chunk.reconcileBinlogEvent(
				Mutation{
					Type:  test.eventType,
					Table: table,
					Rows:  test.eventRows,
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
