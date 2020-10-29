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
	err := insertBunchaData(source, "Name", rowCount)
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
	assert.NoError(t, err)
	shard, err := parseTarget("customer/-80")
	assert.NoError(t, err)
	tables, err := LoadTables(ctx, source.Type, conns[0], shard, nil)
	err = generateTableChunks(ctx, conns[0], tables[0], 10, chunks)
	assert.NoError(t, err)
	close(chunks)
	wg.Wait()

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

func TestChunkerSingleRow(t *testing.T) {
	source := vitessContainer.Config()

	db, err := source.DB()
	assert.NoError(t, err)

	_, err = db.Exec(`
		INSERT INTO customer_passcodes (
			id,
			customer_id,
			original_customer_id,
			token,
			fidelius_token,
			active,
			unlinked_at,
			version,
			created_at,
			updated_at,
			last_verified_at 
		) VALUES (
			100020406,
			30027935561,
			30027935561,
			'P_5hpl5pcdquw95xr34ixy4b7ep',
			'fid-1-0dbd273bb139f524b524e95b8711550ccc78e3558d52c3c3e26c093842543771',
			NULL,
			'2020-10-22 14:33:16',
			18,
			'2020-08-24 13:29:44',
			'2020-10-22 14:33:16',
			'2020-10-22 14:33:16'
		)
	`)
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
	db, err = source.DB()
	assert.NoError(t, err)
	conns, err := OpenConnections(ctx, db, 1)
	assert.NoError(t, err)
	shard, err := parseTarget("customer/-80")
	assert.NoError(t, err)
	tables, err := LoadTables(ctx, source.Type, conns[0], shard, []string{"customer_passcodes"})
	err = generateTableChunks(ctx, conns[0], tables[0], 10, chunks)
	assert.NoError(t, err)
	close(chunks)
	wg.Wait()

	assert.Equal(t, []testChunk{
		{
			Start: 0,
			End:   100020406,
			Size:  1,
			First: true,
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
