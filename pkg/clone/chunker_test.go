package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/mightyguava/autotx"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"strings"
	"testing"
	"time"
)

type testChunk struct {
	Start []int64
	End   []int64
	First bool
	Last  bool
}

func TestChunker(t *testing.T) {
	tests := []struct {
		name    string
		table   string
		config  TableConfig
		columns []string
		rows    [][]interface{}
		chunks  []testChunk
	}{
		{
			name:   "single column empty table",
			table:  "customers",
			config: TableConfig{ChunkSize: 10},
			rows:   nil,
			chunks: nil,
		},
		{
			name:    "single column one row",
			table:   "customers",
			config:  TableConfig{ChunkSize: 10},
			columns: []string{"id", "name"},
			rows: [][]interface{}{
				{1, "Customer #1"},
			},
			chunks: []testChunk{
				{Start: []int64{1}, End: []int64{2}, First: true, Last: true},
			},
		},
		{
			name:    "single column single chunks",
			table:   "customers",
			config:  TableConfig{ChunkSize: 1},
			columns: []string{"id", "name"},
			rows: [][]interface{}{
				{1, "Customer #1"},
				{2, "Customer #2"},
				{3, "Customer #3"},
			},
			chunks: []testChunk{
				{Start: []int64{1}, End: []int64{2}, First: true},
				{Start: []int64{2}, End: []int64{3}},
				{Start: []int64{3}, End: []int64{4}, Last: true},
			},
		},
		{
			name:    "single column partially full last chunk",
			table:   "customers",
			config:  TableConfig{ChunkSize: 2},
			columns: []string{"id", "name"},
			rows: [][]interface{}{
				{1, "Customer #1"},
				{2, "Customer #2"},
				{3, "Customer #3"},
			},
			chunks: []testChunk{
				{Start: []int64{1}, End: []int64{3}, First: true},
				{Start: []int64{3}, End: []int64{4}, Last: true},
			},
		},
		{
			name:    "single column 95 rows",
			table:   "customers",
			config:  TableConfig{ChunkSize: 20},
			columns: []string{"id", "name"},
			rows:    customerRows(95),
			chunks: []testChunk{
				{Start: []int64{1}, End: []int64{21}, First: true},
				{Start: []int64{21}, End: []int64{41}},
				{Start: []int64{41}, End: []int64{61}},
				{Start: []int64{61}, End: []int64{81}},
				{Start: []int64{81}, End: []int64{96}, Last: true},
			},
		},
		{
			name:   "two columns empty table",
			table:  "transactions",
			config: TableConfig{KeyColumns: []string{"customer_id", "id"}, ChunkSize: 10},
			rows:   nil,
			chunks: nil,
		},
		{
			name:    "two columns one row",
			table:   "transactions",
			config:  TableConfig{KeyColumns: []string{"customer_id", "id"}, ChunkSize: 10},
			columns: []string{"customer_id", "id", "description", "amount_cents"},
			rows: [][]interface{}{
				{1, 1, "Description #1", 1000},
			},
			chunks: []testChunk{
				{Start: []int64{1, 1}, End: []int64{1, 2}, First: true, Last: true},
			},
		},
		{
			name:    "two columns page in middle of parent",
			table:   "transactions",
			config:  TableConfig{KeyColumns: []string{"customer_id", "id"}, ChunkSize: 3},
			columns: []string{"customer_id", "id", "description", "amount_cents"},
			rows: [][]interface{}{
				{1, 1, "Description #1", 1000},
				{1, 2, "Description #2", 2000},
				{2, 3, "Description #3", 3000},
				// page break is here
				{2, 4, "Description #4", 4000},
				{2, 5, "Description #5", 5000},
			},
			chunks: []testChunk{
				{Start: []int64{1, 1}, End: []int64{2, 4}, First: true},
				{Start: []int64{2, 4}, End: []int64{2, 6}, Last: true},
			},
		},
		{
			name:    "two columns ids in other order",
			table:   "transactions",
			config:  TableConfig{KeyColumns: []string{"customer_id", "id"}, ChunkSize: 3},
			columns: []string{"customer_id", "id", "description", "amount_cents"},
			rows: [][]interface{}{
				{1, 3, "Description #3", 3000},
				{1, 4, "Description #4", 4000},
				{1, 5, "Description #5", 5000},
				// page break is here
				{2, 1, "Description #1", 1000},
				{2, 2, "Description #2", 2000},
			},
			chunks: []testChunk{
				{Start: []int64{1, 3}, End: []int64{2, 1}, First: true},
				{Start: []int64{2, 1}, End: []int64{2, 3}, Last: true},
			},
		},
		{
			name:    "two columns 95 rows",
			table:   "transactions",
			config:  TableConfig{KeyColumns: []string{"customer_id", "id"}, ChunkSize: 10},
			columns: []string{"customer_id", "id", "description", "amount_cents"},
			rows:    transactionRows(95, 3),
			chunks: []testChunk{
				{Start: []int64{0, 1}, End: []int64{3, 11}, First: true},
				{Start: []int64{3, 11}, End: []int64{7, 21}},
				{Start: []int64{7, 21}, End: []int64{10, 31}},
				{Start: []int64{10, 31}, End: []int64{13, 41}},
				{Start: []int64{13, 41}, End: []int64{17, 51}},
				{Start: []int64{17, 51}, End: []int64{20, 61}},
				{Start: []int64{20, 61}, End: []int64{23, 71}},
				{Start: []int64{23, 71}, End: []int64{27, 81}},
				{Start: []int64{27, 81}, End: []int64{30, 91}},
				{Start: []int64{30, 91}, End: []int64{31, 96}, Last: true}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := startVitess()
			require.NoError(t, err)

			source := vitessContainer.Config()

			ctx := context.Background()

			config := ReaderConfig{ReadTimeout: time.Second, ChunkSize: 10,
				Config: Config{
					Tables: map[string]TableConfig{
						test.table: test.config,
					},
				},
				SourceTargetConfig: SourceTargetConfig{
					Source: source,
				}}

			// Loading table schema is only supported from a specific shard
			schemaConfig := config
			schemaConfig.Source.Database = "customer/-80@replica"
			tables, err := LoadTables(ctx, schemaConfig)
			require.NoError(t, err)

			db, err := config.Source.DB()
			require.NoError(t, err)
			defer db.Close()

			err = deleteAllData(config.Source)
			require.NoError(t, err)
			err = insertRows(ctx, db, test.table, test.columns, test.rows)
			require.NoError(t, err)

			chunks, err := generateTableChunks(ctx, tables[0], db, RetryOptions{Timeout: time.Second, MaxRetries: 1})
			require.NoError(t, err)

			var result []testChunk
			for _, chunk := range chunks {
				result = append(result, toTestChunk(chunk))
			}

			assert.Equal(t, test.chunks, result)

			// Count the rows that we select from the chunks and then also count the rows actually in the shard
			// to make sure we don't miss anything
			retry := RetryOptions{Timeout: time.Second}

			var idsInChunks []int64
			for i, chunk := range chunks {
				buffer, err := bufferChunk(ctx, retry, db, "source", chunk)
				require.NoError(t, err)
				rows, err := readAll(buffer)
				require.NoError(t, err)
				for _, row := range rows {
					idsInChunks = append(idsInChunks, row.ID)
				}
				if i < len(chunks)-1 {
					assert.Equal(t, tables[0].Config.ChunkSize, len(rows))
				}
			}
			keyColumns := test.config.KeyColumns
			if keyColumns == nil {
				keyColumns = []string{"id"}
			}
			rows, err := db.QueryContext(ctx, fmt.Sprintf("SELECT %s FROM %s ORDER BY %s",
				strings.Join(keyColumns, ","),
				test.table,
				strings.Join(keyColumns, ",")))
			require.NoError(t, err)
			var idsInDB []int64
			row := make([]int64, len(keyColumns))
			scanArgs := make([]interface{}, len(keyColumns))
			for i := range row {
				scanArgs[i] = &row[i]
			}
			for rows.Next() {
				err = rows.Scan(scanArgs...)
				require.NoError(t, err)
				idsInDB = append(idsInDB, row[len(row)-1])
			}
			assert.Equal(t, idsInDB, idsInChunks)
		})
	}
}

func customerRows(count int) [][]interface{} {
	result := make([][]interface{}, count)
	for i := range result {
		result[i] = []interface{}{
			i + 1, fmt.Sprintf("Customer #%d", i+1),
		}
	}
	return result
}

func transactionRows(count int, transactionsPerCustomer int) [][]interface{} {
	result := make([][]interface{}, count)
	for i := range result {
		ordinal := i + 1
		customerId := ordinal / transactionsPerCustomer
		result[i] = []interface{}{
			customerId, ordinal, fmt.Sprintf("Description #%d", ordinal), ordinal * 1000,
		}
	}
	return result
}

func insertRows(ctx context.Context, db *sql.DB, table string, columns []string, rows [][]interface{}) error {
	if len(rows) == 0 {
		return nil
	}
	return autotx.Transact(ctx, db, func(tx *sql.Tx) error {
		var questionMarks strings.Builder
		for i := range rows[0] {
			questionMarks.WriteString("?")
			if i < len(rows[0])-1 {
				questionMarks.WriteString(", ")
			}
		}
		s := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table, strings.Join(columns, ", "), questionMarks.String())
		stmt, err := tx.PrepareContext(ctx, s)
		if err != nil {
			return errors.WithStack(err)
		}
		for _, row := range rows {
			_, err := stmt.ExecContext(ctx, row...)
			if err != nil {
				return errors.Wrapf(err, "could not execute: %s", s)
			}
		}
		return nil
	})
}

func toTestChunk(chunk Chunk) testChunk {
	var start []int64
	for _, v := range chunk.Start {
		start = append(start, v.(int64))
	}
	var end []int64
	for _, v := range chunk.End {
		end = append(end, v.(int64))
	}
	return testChunk{
		Start: start,
		End:   end,
		First: chunk.First,
		Last:  chunk.Last,
	}
}
