package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"io"
	"reflect"
	"strings"
	"sync"
)

// Chunk is an chunk of rows closed to the left [start,end)
type Chunk struct {
	Table *Table

	// Seq is the sequence number of chunks for this table
	Seq int64

	// Start is the first id of the chunk inclusive
	Start []interface{}
	// End is the first id of the next chunk (i.e. the last id of this chunk exclusively)
	End []interface{} // exclusive

	// First chunk of a table
	First bool
	// Last chunk of a table
	Last bool

	// Size is the expected number of rows in the chunk
	Size int
}

func (c *Chunk) String() string {
	return fmt.Sprintf("%s[%v-%v]", c.Table.Name, c.Start, c.End)
}

func (c *Chunk) ContainsRow(row []interface{}) bool {
	id := c.Table.PkOfRow(row)
	return c.ContainsPK(id)
}

func (c *Chunk) ContainsPK(id int64) bool {
	pkColumn := c.Table.MysqlTable.GetPKColumn(0)
	for i, column := range c.Table.ChunkColumns {
		if pkColumn.Name == column {
			return genericCompare(id, c.Start[i]) >= 0 && genericCompare(id, c.End[i]) < 0
		}
	}
	panic("chunk columns does not contain the pk column")
}

func genericCompare(a interface{}, b interface{}) int {
	// Different database drivers interpret SQL types differently (it seems)
	aType := reflect.TypeOf(a)
	bType := reflect.TypeOf(b)

	// If they do NOT have same type, we coerce the target type to the source type and then compare
	// We only support the combinations we've encountered in the wild here
	switch a := a.(type) {
	case int64:
		coerced, err := coerceInt64(b)
		if err != nil {
			panic(err)
		}
		if a == coerced {
			return 0
		} else if a < coerced {
			return -1
		} else {
			return 1
		}
	case uint64:
		coerced, err := coerceUint64(b)
		if err != nil {
			panic(err)
		}
		if a == coerced {
			return 0
		} else if a < coerced {
			return -1
		} else {
			return 1
		}
	case float64:
		coerced, err := coerceFloat64(b)
		if err != nil {
			panic(err)
		}
		if a == coerced {
			return 0
		} else if a < coerced {
			return -1
		} else {
			return 1
		}
	case string:
		coerced, err := coerceString(b)
		if err != nil {
			panic(err)
		}
		if a == coerced {
			return 0
		} else if a < coerced {
			return -1
		} else {
			return 1
		}
	default:
		panic(fmt.Sprintf("type combination %v -> %v not supported yet: source=%v target=%v",
			aType, bType, a, b))
	}
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
	Next(context.Context) ([]interface{}, bool, error)
}

type peekingIdStreamer struct {
	wrapped   IdStreamer
	peeked    []interface{}
	hasPeeked bool
}

func (p *peekingIdStreamer) Next(ctx context.Context) ([]interface{}, bool, error) {
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
	Next(context.Context) ([]interface{}, error)
}

type pagingStreamer struct {
	conn         DBReader
	first        bool
	currentPage  [][]interface{}
	currentIndex int
	pageSize     int
	retry        RetryOptions

	chunkColumns []string

	firstStatement  string
	offsetStatement string
}

func newPagingStreamer(conn DBReader, table *Table, pageSize int, retry RetryOptions) *pagingStreamer {
	p := &pagingStreamer{
		conn:         conn,
		retry:        retry,
		first:        true,
		pageSize:     pageSize,
		currentPage:  nil,
		currentIndex: 0,
		chunkColumns: table.ChunkColumns,
	}
	var chunkColumnList strings.Builder
	var params strings.Builder
	var comparison strings.Builder
	for i, column := range table.ChunkColumns {
		chunkColumnList.WriteString("`")
		chunkColumnList.WriteString(column)
		chunkColumnList.WriteString("`")
		params.WriteString("?")
		comparison.WriteString("`")
		comparison.WriteString(column)
		comparison.WriteString("`")
		comparison.WriteString(" >= ?")
		if i < len(table.ChunkColumns)-1 {
			chunkColumnList.WriteString(", ")
			params.WriteString(", ")
			comparison.WriteString(" and ")
		}
	}

	p.firstStatement = fmt.Sprintf("select %s from %s order by %s limit %d",
		chunkColumnList.String(), table.Name, chunkColumnList.String(), p.pageSize)

	// We both compare by the columns directly so that indexes can be used and then by the concatenation for correctness
	p.offsetStatement = fmt.Sprintf("select %s from %s where %s and concat(%s) > concat(%s) order by %s",
		chunkColumnList.String(), table.Name, comparison.String(), chunkColumnList.String(), params.String(), chunkColumnList.String())

	return p
}

func (p *pagingStreamer) Next(ctx context.Context) ([]interface{}, error) {
	if p.currentIndex == len(p.currentPage) {
		var err error
		p.currentPage, err = p.loadPage(ctx)
		if errors.Is(err, io.EOF) {
			// Race condition, the table was emptied
			return nil, io.EOF
		}
		if err != nil {
			return nil, errors.WithStack(err)
		}
		p.currentIndex = 0
	}
	if len(p.currentPage) == 0 {
		return nil, io.EOF
	}
	next := p.currentPage[p.currentIndex]
	p.currentIndex++
	return next, nil
}

func (p *pagingStreamer) loadPage(ctx context.Context) ([][]interface{}, error) {
	var result [][]interface{}
	err := Retry(ctx, p.retry, func(ctx context.Context) error {
		var err error

		result = make([][]interface{}, 0, p.pageSize)
		var rows *sql.Rows
		if p.first {
			p.first = false
			rows, err = p.conn.QueryContext(ctx, p.firstStatement)
			if err != nil {
				return errors.Wrapf(err, "could not execute query: %v", p.firstStatement)
			}
			defer rows.Close()
		} else {
			result = nil
			if len(p.currentPage) == 0 {
				// Race condition, the table was emptied
				return backoff.Permanent(io.EOF)
			}
			lastItem := p.currentPage[len(p.currentPage)-1]
			params := make([]interface{}, 0, 2*len(p.currentPage))
			// First for the per column comparison for index use efficiency
			params = append(params, lastItem...)
			// Then for the concatenation for correctness
			params = append(params, lastItem...)
			rows, err = p.conn.QueryContext(ctx, p.offsetStatement, params...)
			if err != nil {
				return errors.Wrapf(err, "could not execute query: %v", p.offsetStatement)
			}
			defer rows.Close()
		}
		for rows.Next() {
			item := make([]interface{}, len(p.chunkColumns))
			scanArgs := make([]interface{}, len(item))
			for i := range item {
				scanArgs[i] = &item[i]
			}
			err := rows.Scan(scanArgs...)
			if err != nil {
				return errors.WithStack(err)
			}
			result = append(result, item)
		}
		return err
	})

	return result, err
}

func streamIds(conn DBReader, table *Table, pageSize int, retry RetryOptions) PeekingIdStreamer {
	return &peekingIdStreamer{
		wrapped: newPagingStreamer(conn, table, pageSize, retry),
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
	var startId []interface{}
	seq := int64(0)
	var id []interface{}
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

		if startId == nil {
			startId = id
		}

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
		chunksEnqueued.WithLabelValues(table.Name).Inc()
		// Make sure the End position is _after_ the final row by "adding one" to it
		incChunkPosition(id)
		select {
		case chunks <- Chunk{
			Table: table,
			Seq:   seq,
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

func incChunkPosition(pos []interface{}) {
	inc, err := increment(pos[len(pos)-1])
	if err != nil {
		panic(err)
	}
	pos[len(pos)-1] = inc
}

func increment(value interface{}) (interface{}, error) {
	switch value := value.(type) {
	case int64:
		return value + 1, nil
	default:
		return 0, errors.Errorf("can't (yet?) increment %v: %v", reflect.TypeOf(value), value)
	}
}
