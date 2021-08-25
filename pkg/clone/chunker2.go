package clone

import (
	"context"
	"database/sql"
	"github.com/pkg/errors"
	"io"
)

// Chunk2 is an chunk of rows closed to the left [start,end)
type Chunk2 struct {
	Table *Table

	// Seq is the sequence number of chunks for this table
	Seq int64

	// Start is the first id of the chunk inclusive
	Start int64
	// End is the first id of the next chunk (i.e. the last id of this chunk exclusive)
	End int64 // exclusive
}

func (c *Chunk2) ContainsRow(row []interface{}) bool {
	id := c.Table.PkOfRow(row)
	return id >= c.Start && id < c.End
}

func generateTableChunks2(ctx context.Context, table *Table, source *sql.DB, retry RetryOptions) ([]Chunk2, error) {
	chunkSize := table.Config.ChunkSize

	ids := streamIds(source, table, chunkSize, retry)

	var err error
	var chunks []Chunk2
	currentChunkSize := 0
	first := true
	startId := int64(0)
	seq := int64(0)
	var id int64
	hasNext := true
	for hasNext {
		id, hasNext, err = ids.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil, nil
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
		currentChunkSize++

		if currentChunkSize == chunkSize {
			chunksEnqueued.WithLabelValues(table.Name).Inc()
			chunks = append(chunks, Chunk2{
				Table: table,
				Seq:   seq,
				Start: startId,
				End:   id,
			})
			seq++
			// Next id should be the next start id
			startId = id
			// We are no longer the first chunk
			first = false
			// We have no rows in the next chunk yet
			currentChunkSize = 0
		}
	}
	// Send any partial chunk we might have
	if currentChunkSize > 0 {
		if first {
			// This is the first AND last chunk, the startId doesn't make sense because we never got a second chunk
			startId = 0
		}
		chunksEnqueued.WithLabelValues(table.Name).Inc()
		chunks = append(chunks, Chunk2{
			Table: table,
			Seq:   seq,
			Start: startId,
			End:   id + 1,
		})
	}
	return chunks, nil
}
