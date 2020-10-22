package clone

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestChunker(t *testing.T) {
	source := vitessContainer.Config()

	rowCount := 1000
	err := insertBunchaData(source, rowCount)
	assert.NoError(t, err)

	source.Database = "customer/-80@replica"

	chunks := make(chan Chunk)
	var result []Chunk
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for chunk := range chunks {
			result = append(result, chunk)
		}
	}()

	ctx := context.Background()
	db, err := source.DB()
	assert.NoError(t, err)
	conns, err := OpenConnections(ctx, db, 1)
	tables, err := LoadTables(ctx, source.Type, conns[0], "customer", nil)
	err = GenerateChunks(ctx, conns, tables, 50, chunks)
	assert.NoError(t, err)

	assert.Equal(t, []Chunk{}, result)
}
