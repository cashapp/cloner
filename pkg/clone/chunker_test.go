package clone

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

type testChunk struct {
	Start int64
	End   int64
}

func TestChunker(t *testing.T) {
	err := startVitess()
	assert.NoError(t, err)

	source := vitessContainer.Config()

	err = deleteAllData(source)
	assert.NoError(t, err)

	rowCount := 100
	err = insertBunchaData(source, "Name", rowCount)
	assert.NoError(t, err)

	source.Database = "customer/-80@replica"

	ctx := context.Background()
	config := ReaderConfig{ReadTimeout: time.Second, ChunkSize: 10, SourceTargetConfig: SourceTargetConfig{Source: source}}

	tables, err := LoadTables(ctx, config)
	assert.NoError(t, err)
	assert.Equal(t, 10, tables[0].Config.ChunkSize)

	db, err := config.Source.DB()
	assert.NoError(t, err)
	defer db.Close()
	chunks, err := generateTableChunks(ctx, tables[0], db, RetryOptions{Timeout: time.Second, MaxRetries: 1})
	assert.NoError(t, err)

	result := make([]testChunk, len(chunks))
	for i, chunk := range chunks {
		result[i] = toTestChunk2(chunk)
	}

	assert.Equal(t, []testChunk{
		{
			Start: 0,
			End:   21,
		},
		{
			Start: 21,
			End:   41,
		},
		{
			Start: 41,
			End:   56,
		},
		{
			Start: 56,
			End:   73,
		},
		{
			Start: 73,
			End:   95,
		},
		{
			Start: 95,
			End:   100,
		},
	}, result)
}

func TestChunkerEmptyTable(t *testing.T) {
	err := startVitess()
	assert.NoError(t, err)

	source := vitessContainer.Config()

	err = deleteAllData(source)
	assert.NoError(t, err)

	source.Database = "customer/-80@replica"

	config := ReaderConfig{ReadTimeout: time.Second,
		Config: Config{
			Tables: map[string]TableConfig{
				"customers": {},
			},
		},
		ChunkSize:          10,
		SourceTargetConfig: SourceTargetConfig{Source: source}}

	ctx := context.Background()
	tables, err := LoadTables(ctx, config)
	assert.NoError(t, err)

	db, err := config.Source.DB()
	assert.NoError(t, err)
	defer db.Close()
	chunks, err := generateTableChunks(ctx, tables[0], db, RetryOptions{Timeout: time.Second, MaxRetries: 1})
	assert.NoError(t, err)

	assert.Equal(t, 0, len(chunks))
}

func TestChunkerSingleRow(t *testing.T) {
	err := startVitess()
	assert.NoError(t, err)

	source := vitessContainer.Config()

	err = deleteAllData(source)
	assert.NoError(t, err)

	err = insertBunchaData(source, "Jon", 1)
	assert.NoError(t, err)

	source.Database = "customer/-80@replica"

	ctx := context.Background()
	config := ReaderConfig{ReadTimeout: time.Second,
		Config: Config{
			Tables: map[string]TableConfig{
				"customers": {},
			},
		},
		ChunkSize:          10,
		SourceTargetConfig: SourceTargetConfig{Source: source}}

	tables, err := LoadTables(ctx, config)
	assert.NoError(t, err)

	db, err := config.Source.DB()
	assert.NoError(t, err)
	defer db.Close()
	chunks, err := generateTableChunks(ctx, tables[0], db, RetryOptions{Timeout: time.Second, MaxRetries: 1})
	assert.NoError(t, err)
	var result []testChunk
	for _, chunk := range chunks {
		result = append(result, toTestChunk2(chunk))
	}

	assert.Equal(t, []testChunk{
		{
			Start: 0,
			End:   3,
		},
	}, result)
}

func toTestChunk2(chunk Chunk) testChunk {
	return testChunk{
		Start: chunk.Start,
		End:   chunk.End,
	}
}
