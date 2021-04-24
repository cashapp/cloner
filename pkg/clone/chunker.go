package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"io"
	"time"

	"github.com/pkg/errors"
)

// Chunk is an chunk of rows closed to the left [start,end)
type Chunk struct {
	Table *Table
	// Start is the first id of the chunk inclusive
	Start int64
	// End is the first id of the next chunk (i.e. the last id of this chunk exclusive)
	End int64 // exclusive

	// First chunk of a table
	First bool
	// Last chunk of a table
	Last bool
	// Size is the expected number of rows in the chunk
	Size int
}

//func GenerateTableChunks(ctx context.Context, conn *sql.Conn, table *Table, chunkSize int, chunks chan Chunk) error {
//	log.Infof("Generating chunks for %v", table.Name)
//	sql := fmt.Sprintf(`
//		select
//			  min(tmp.id) as start_rowid,
//			  max(tmp.id) as end_rowid,
//			  count(*) as page_size
//		from (
//		  select
//			id,
//			row_number() over (order by id) as row_num
//		  from %s
//		) tmp
//		group by floor((tmp.row_num - 1) / ?)
//		order by start_rowid
//	`, table.Name)
//	rows, err := conn.QueryContext(ctx, sql, chunkSize)
//	if err != nil {
//		return errors.WithStack(err)
//	}
//	defer rows.Close()
//	for rows.Next() {
//		var startId int64
//		var endId int64
//		var pageSize int64
//		err := rows.Scan(&startId, &endId, &pageSize)
//		if err != nil {
//			return errors.WithStack(err)
//		}
//		chunks <- Chunk{
//			Table: table,
//			Start: startId,
//			End:   endId,
//		}
//	}
//	return nil
//}

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
	timeout      time.Duration
	first        bool
	currentPage  []int64
	currentIndex int
	table        *Table
	pageSize     int
	retries      uint64
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
	err := Retry(ctx, p.retries, p.timeout, func(ctx context.Context) error {
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

func streamIds(conn DBReader, table *Table, timeout time.Duration, pageSize int, retries uint64) PeekingIdStreamer {
	return &peekingIdStreamer{
		wrapped: &pagingStreamer{
			conn:         conn,
			timeout:      timeout,
			retries:      retries,
			first:        true,
			pageSize:     pageSize,
			currentPage:  nil,
			currentIndex: 0,
			table:        table,
		},
	}
}

func GenerateTableChunks(
	ctx context.Context,
	config ReaderConfig,
	conn DBReader,
	table *Table,
	chunks chan Chunk,
) error {
	chunkSize := table.Config.ChunkSize
	if chunkSize == 0 {
		chunkSize = config.ChunkSize
	}
	ids := streamIds(conn, table, config.ReadTimeout, chunkSize, config.ReadRetries)

	var err error
	currentChunkSize := 0
	first := true
	startId := int64(0)
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
				Start: startId,
				End:   id,
				First: first,
				Last:  !hasNext,
				Size:  currentChunkSize,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
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
			Start: startId,
			End:   id,
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
