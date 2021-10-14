package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"io"
	"sync"
)

// Chunk is an chunk of rows closed to the left [start,end)
type Chunk struct {
	Table *Table

	// Seq is the sequence number of chunks for this table
	Seq int64

	// Start is the first id of the chunk inclusive
	Start int64
	// End is the first id of the next chunk (i.e. the last id of this chunk exclusively)
	End int64 // exclusive

	// First chunk of a table
	First bool
	// Last chunk of a table
	Last bool

	// Size is the expected number of rows in the chunk
	Size int
}

func (c *Chunk) ContainsRow(row []interface{}) bool {
	id := c.Table.PkOfRow(row)
	return c.ContainsPK(id)
}

func (c *Chunk) ContainsPK(id int64) bool {
	return id >= c.Start && id < c.End
}

func (c *Chunk) ContainsPKs(pk []interface{}) bool {
	// TODO when we support arbitrary primary keys this logic has to change
	if len(pk) != 1 {
		panic("currently only supported single integer pk")
	}
	i, err := coerceInt64(pk[0])
	if err != nil {
		panic(err)
	}
	return c.ContainsPK(i)
}

type PeekingIdStreamer interface {
	// Next returns next id and a boolean indicating if there is a next after this one
	Next(context.Context) (int64, bool, error)
}

type peekingIdStreamer struct {
	wrapped   IdStreamer
	peeked    int64
	hasPeeked bool
}

func (p *peekingIdStreamer) Next(ctx context.Context) (int64, bool, error) {
	var err error
	if !p.hasPeeked {
		// first time round load the first entry
		p.peeked, err = p.wrapped.Next(ctx)
		if errors.Is(err, io.EOF) {
			return p.peeked, false, err
		} else {
			if err != nil {
				return p.peeked, false, errors.WithStack(err)
			}
		}
		p.hasPeeked = true
	}

	next := p.peeked
	hasNext := true

	p.peeked, err = p.wrapped.Next(ctx)
	if errors.Is(err, io.EOF) {
		hasNext = false
	} else {
		if err != nil {
			return next, hasNext, errors.WithStack(err)
		}
	}
	return next, hasNext, nil
}

type IdStreamer interface {
	Next(context.Context) (int64, error)
}

type pagingStreamer struct {
	conn         DBReader
	first        bool
	currentPage  []int64
	currentIndex int
	table        *Table
	pageSize     int
	retry        RetryOptions
}

func (p *pagingStreamer) Next(ctx context.Context) (int64, error) {
	if p.currentIndex == len(p.currentPage) {
		var err error
		p.currentPage, err = p.loadPage(ctx)
		if errors.Is(err, io.EOF) {
			// Race condition, the table was emptied
			return 0, io.EOF
		}
		if err != nil {
			return 0, errors.WithStack(err)
		}
		p.currentIndex = 0
	}
	if len(p.currentPage) == 0 {
		return 0, io.EOF
	}
	next := p.currentPage[p.currentIndex]
	p.currentIndex++
	return next, nil
}

func (p *pagingStreamer) loadPage(ctx context.Context) ([]int64, error) {
	var result []int64
	err := Retry(ctx, p.retry, func(ctx context.Context) error {
		var err error

		result = make([]int64, 0, p.pageSize)
		var rows *sql.Rows
		if p.first {
			p.first = false
			rows, err = p.conn.QueryContext(ctx, fmt.Sprintf("select %s from %s order by %s asc limit %d",
				p.table.IDColumn, p.table.Name, p.table.IDColumn, p.pageSize))
			if err != nil {
				return errors.WithStack(err)
			}
			defer rows.Close()
		} else {
			result = nil
			if len(p.currentPage) == 0 {
				// Race condition, the table was emptied
				return backoff.Permanent(io.EOF)
			}
			lastId := p.currentPage[len(p.currentPage)-1]
			rows, err = p.conn.QueryContext(ctx, fmt.Sprintf("select %s from %s where %s > %d order by %s asc limit %d",
				p.table.IDColumn, p.table.Name, p.table.IDColumn, lastId, p.table.IDColumn, p.pageSize))
			if err != nil {
				return errors.WithStack(err)
			}
			defer rows.Close()
		}
		for rows.Next() {
			var id int64
			err := rows.Scan(&id)
			if err != nil {
				return errors.WithStack(err)
			}
			result = append(result, id)
		}
		return err
	})

	return result, err
}

func streamIds(conn DBReader, table *Table, pageSize int, retry RetryOptions) PeekingIdStreamer {
	return &peekingIdStreamer{
		wrapped: &pagingStreamer{
			conn:         conn,
			retry:        retry,
			first:        true,
			pageSize:     pageSize,
			currentPage:  nil,
			currentIndex: 0,
			table:        table,
		},
	}
}

func generateTableChunks(ctx context.Context, table *Table, source *sql.DB, retry RetryOptions) ([]Chunk, error) {
	var chunks []Chunk
	chunkCh := make(chan Chunk)
	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for c := range chunkCh {
			chunks = append(chunks, c)
		}
	}()
	err := generateTableChunksAsync(ctx, table, source, chunkCh, retry)
	close(chunkCh)
	if err != nil {
		return chunks, errors.WithStack(err)
	}
	wg.Wait()
	return chunks, nil
}

// generateTableChunksAsync generates chunks async on the current goroutine
func generateTableChunksAsync(ctx context.Context, table *Table, source *sql.DB, chunks chan Chunk, retry RetryOptions) error {
	chunkSize := table.Config.ChunkSize

	ids := streamIds(source, table, chunkSize, retry)

	var err error
	currentChunkSize := 0
	first := true
	startId := int64(0)
	seq := int64(0)
	var id int64
	hasNext := true
	for hasNext {
		id, hasNext, err = ids.Next(ctx)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		currentChunkSize++

		if currentChunkSize == chunkSize {
			chunksEnqueued.WithLabelValues(table.Name).Inc()
			select {
			case chunks <- Chunk{
				Table: table,
				Seq:   seq,
				Start: startId,
				End:   id,
				First: first,
				Last:  !hasNext,
				Size:  currentChunkSize,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
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
		select {
		case chunks <- Chunk{
			Table: table,
			Seq:   seq,
			Start: startId,
			End:   id + 1,
			First: first,
			Last:  true,
			Size:  currentChunkSize,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}
