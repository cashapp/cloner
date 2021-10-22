package clone

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
		name     string
		table    string
		config   TableConfig
		rowCount int
		chunks   []testChunk
	}{
		{
			name:     "single column empty table",
			table:    "customers",
			config:   TableConfig{ChunkSize: 10},
			rowCount: 0,
			chunks:   nil,
		},
		{
			name:     "single column one row",
			table:    "customers",
			config:   TableConfig{ChunkSize: 10},
			rowCount: 1,
			chunks: []testChunk{
				{
					Start: []int64{2},
					End:   []int64{3},
					First: true,
					Last:  true,
				},
			},
		},
		{
			name:     "single column hundred rows",
			table:    "customers",
			config:   TableConfig{ChunkSize: 10},
			rowCount: 100,
			chunks: []testChunk{
				{
					Start: []int64{2},
					End:   []int64{21},
					First: true,
				},
				{
					Start: []int64{21},
					End:   []int64{41},
				},
				{
					Start: []int64{41},
					End:   []int64{56},
				},
				{
					Start: []int64{56},
					End:   []int64{73},
				},
				{
					Start: []int64{73},
					End:   []int64{95},
				},
				{
					Start: []int64{95},
					End:   []int64{100},
					Last:  true,
				},
			},
		},
		{
			name:     "two columns empty table",
			table:    "transactions",
			config:   TableConfig{ChunkColumns: []string{"customer_id", "id"}, ChunkSize: 10},
			rowCount: 0,
			chunks:   nil,
		},
		{
			name:     "two columns one row",
			table:    "transactions",
			config:   TableConfig{ChunkColumns: []string{"customer_id", "id"}, ChunkSize: 10},
			rowCount: 1,
			chunks: []testChunk{
				{
					Start: []int64{2, 1},
					End:   []int64{2, 2},
					First: true,
					Last:  true,
				},
			},
		},
		{
			name:     "two columns 100 rows",
			table:    "transactions",
			config:   TableConfig{ChunkColumns: []string{"customer_id", "id"}, ChunkSize: 10},
			rowCount: 100,
			chunks: []testChunk{
				{
					Start: []int64{2, 1},
					End:   []int64{21, 20},
					First: true,
				},
				{
					Start: []int64{21, 20},
					End:   []int64{41, 40},
				},
				{
					Start: []int64{41, 40},
					End:   []int64{56, 55},
				},
				{
					Start: []int64{56, 55},
					End:   []int64{73, 72},
				},
				{
					Start: []int64{73, 72},
					End:   []int64{95, 94},
				},
				{
					Start: []int64{95, 94},
					End:   []int64{99, 99},
					Last:  true,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := startVitess()
			require.NoError(t, err)

			source := vitessContainer.Config()

			ctx := context.Background()

			err = insertBunchaData(ctx, source, test.rowCount)
			require.NoError(t, err)

			source.Database = "customer/-80@replica"

			config := ReaderConfig{ReadTimeout: time.Second, ChunkSize: 10,
				Config: Config{
					Tables: map[string]TableConfig{
						test.table: test.config,
					},
				},
				SourceTargetConfig: SourceTargetConfig{
					Source: source,
				}}

			tables, err := LoadTables(ctx, config)
			require.NoError(t, err)
			assert.Equal(t, 10, tables[0].Config.ChunkSize)

			db, err := config.Source.DB()
			require.NoError(t, err)
			defer db.Close()
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

			rowsInChunks := 0
			for _, chunk := range chunks {
				buffer, err := bufferChunk(ctx, retry, db, "source", chunk)
				require.NoError(t, err)
				rows, err := readAll(buffer)
				require.NoError(t, err)
				rowsInChunks += len(rows)
			}
			leftShardConfig := config.Source
			leftShardConfig.Database = "customer/-80"
			leftShardDB, err := leftShardConfig.DB()
			require.NoError(t, err)
			countRow := leftShardDB.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %v", test.table))
			var rowsInShard int
			err = countRow.Scan(&rowsInShard)
			require.NoError(t, err)
			assert.Equal(t, rowsInShard, rowsInChunks)
		})
	}
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
