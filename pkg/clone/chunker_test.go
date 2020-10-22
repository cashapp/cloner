package clone

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testChunk struct {
	Start int64
	End   int64

	First bool
	Last  bool
	Size  int
}

func TestChunker(t *testing.T) {
	source := vitessContainer.Config()

	rowCount := 100
	err := insertBunchaData(source, rowCount)
	assert.NoError(t, err)

	source.Database = "customer/-80@replica"

	chunks := make(chan Chunk)
	var result []testChunk
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for chunk := range chunks {
			result = append(result, toTestChunk(chunk))
		}
	}()

	ctx := context.Background()
	db, err := source.DB()
	assert.NoError(t, err)
	conns, err := OpenConnections(ctx, db, 1)
	tables, err := LoadTables(ctx, source.Type, conns[0], "customer", nil)
	err = generateTableChunks(ctx, conns[0], tables[0], 10, chunks)
	assert.NoError(t, err)

	assert.Equal(t, []testChunk{
		{
			Start: 0,
			End:   21,
			Size:  10,
			First: true,
		},
		{
			Start: 21,
			End:   41,
			Size:  10,
		},
		{
			Start: 41,
			End:   56,
			Size:  10,
		},
		{
			Start: 56,
			End:   73,
			Size:  10,
		},
		{
			Start: 73,
			End:   95,
			Size:  10,
		},
		{
			Start: 95,
			End:   99,
			Size:  2,
			Last:  true,
		},
	}, result)
}

func toTestChunk(chunk Chunk) testChunk {
	return testChunk{
		Start: chunk.Start,
		End:   chunk.End,
		First: chunk.First,
		Last:  chunk.Last,
		Size:  chunk.Size,
	}
}
