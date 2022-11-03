package clone

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"reflect"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Chunk is an chunk of rows closed to the left [start,end)
type Chunk struct {
	Table *Table

	// Seq is the sequence number of chunks for this table
	Seq int64

	// Start is the first id of the chunk inclusive. If nil, chunk starts at -inf.
	Start []interface{}
	// End is the first id of the next chunk (i.e. the last id of this chunk exclusively). If nil, chunk ends at +inf.
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
	return c.ContainsKeys(c.Table.KeysOfRow(row))
}

func (c *Chunk) ContainsKeys(keys []interface{}) bool {
	result := true
	if c.Start != nil {
		result = result && genericCompareKeys(keys, c.Start) >= 0
	}
	if c.End != nil {
		result = result && genericCompareKeys(keys, c.End) < 0
	}
	return result
}

func genericCompareKeys(a []interface{}, b []interface{}) int {
	for i := range a {
		compare := genericCompare(a[i], b[i])
		if compare != 0 {
			return compare
		}
	}
	return 0
}

func genericCompare(a interface{}, b interface{}) int {
	// Different database drivers interpret SQL types differently (it seems)
	aType := reflect.TypeOf(a)
	bType := reflect.TypeOf(b)

	// If they do NOT have same type, we coerce the target type to the source type and then compare
	// We only support the combinations we've encountered in the wild here
	switch a := a.(type) {
	case int:
		coerced, err := coerceInt64(b)
		if err != nil {
			panic(err)
		}
		if a == int(coerced) {
			return 0
		} else if a < int(coerced) {
			return -1
		} else {
			return 1
		}
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
	case []byte:
		coerced, err := coerceRaw(b)
		if err != nil {
			panic(err)
		}
		return bytes.Compare(a, coerced)
	default:
		panic(fmt.Sprintf("type combination %v -> %v not supported yet: source=%v target=%v",
			aType, bType, a, b))
	}
}

type PeekingIdStreamer interface {
	// Next returns next id and a boolean indicating if there is a next after this one
	Next(context.Context) ([]interface{}, bool, error)
	// Peek returns the id ahead of the current, Next above has to be called first
	Peek() []interface{}
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

func (p *peekingIdStreamer) Peek() []interface{} {
	return p.peeked
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

	table      string
	keyColumns []string
}

func newPagingStreamer(conn DBReader, table *Table, pageSize int, retry RetryOptions) *pagingStreamer {
	p := &pagingStreamer{
		conn:         conn,
		retry:        retry,
		first:        true,
		pageSize:     pageSize,
		currentPage:  nil,
		currentIndex: 0,
		keyColumns:   table.KeyColumns,
		table:        table.Name,
	}

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
			keyColumns := strings.Join(p.keyColumns, ", ")
			stmt := fmt.Sprintf("select %s from %s order by %s limit %d",
				keyColumns, p.table, keyColumns, p.pageSize)
			rows, err = p.conn.QueryContext(ctx, stmt)
			if err != nil {
				return errors.Wrapf(err, "could not execute query: %v", stmt)
			}
			defer rows.Close()
		} else {
			result = nil
			if len(p.currentPage) == 0 {
				// Race condition, the table was emptied
				return backoff.Permanent(io.EOF)
			}
			lastItem := p.currentPage[len(p.currentPage)-1]
			comparison, params := expandRowConstructorComparison(p.keyColumns, ">", lastItem)
			keyColumns := strings.Join(p.keyColumns, ", ")
			stmt := fmt.Sprintf("select %s from %s where %s order by %s limit %d",
				keyColumns, p.table, comparison, keyColumns, p.pageSize)
			rows, err = p.conn.QueryContext(ctx, stmt, params...)
			if err != nil {
				return errors.Wrapf(err, "could not execute query: %v", stmt)
			}
			defer rows.Close()
		}
		for rows.Next() {
			scanArgs := make([]interface{}, len(p.keyColumns))
			for i := range scanArgs {
				// TODO support other types here
				scanArgs[i] = new(int64)
			}
			err := rows.Scan(scanArgs...)
			if err != nil {
				return errors.WithStack(err)
			}
			item := make([]interface{}, len(p.keyColumns))
			for i := range scanArgs {
				item[i] = *scanArgs[i].(*int64)
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
	logger := log.WithContext(ctx).WithField("task", "chunker")
	logger = logger.WithField("table", table.Name)
	logger.Infof("chunking start: %s", table.Name)
	startTime := time.Now()

	chunkSize := table.Config.ChunkSize

	ids := streamIds(source, table, chunkSize, retry)

	var err error
	currentChunkSize := 0
	var startId []interface{}
	seq := int64(0)
	var id []interface{}
	hasNext := true
	for hasNext {
		id, hasNext, err = ids.Next(ctx)
		if errors.Is(err, io.EOF) {
			if startId == nil {
				// The table is empty.
				// Emit a special chunk covering entire keyspace.
				chunks <- Chunk{
					Table: table,
					Seq:   0,
					Start: nil,
					End:   nil,
					First: true,
					Last:  true,
					Size:  0,
				}
			}
			return nil
		}
		if err != nil {
			return errors.WithStack(err)
		}
		currentChunkSize++

		if startId == nil {
			startId = id
			// This is the minimum source ID.
			// Emit a special chunk covering items with keys smaller than minimum ID.
			chunks <- Chunk{
				Table: table,
				Seq:   seq,
				Start: nil,
				End:   startId,
				First: true,
				Size:  0,
			}
		}

		if currentChunkSize == chunkSize {
			chunksEnqueued.WithLabelValues(table.Name).Inc()
			nextId := ids.Peek()
			if !hasNext {
				nextId = nextChunkPosition(id)
			}
			select {
			case chunks <- Chunk{
				Table: table,
				Seq:   seq,
				Start: startId,
				End:   nextId,
				Size:  currentChunkSize,
			}:
			case <-ctx.Done():
				return ctx.Err()
			}
			seq++
			// Next id should be the next start id
			startId = nextId
			// We have no rows in the next chunk yet
			currentChunkSize = 0
		}
	}
	// Send any partial chunk we might have
	if currentChunkSize > 0 {
		chunksEnqueued.WithLabelValues(table.Name).Inc()
		select {
		case chunks <- Chunk{
			Table: table,
			Seq:   seq,
			Start: startId,
			// Make sure the End position is _after_ the final row by "adding one" to it
			End:  nextChunkPosition(id),
			Size: currentChunkSize,
		}:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	// Emit a special chunk covering items with keys greater than maximum ID.
	chunks <- Chunk{
		Table: table,
		Seq:   seq,
		Start: nextChunkPosition(id),
		End:   nil,
		Last:  true,
		Size:  0,
	}

	logger.Infof("chunking done: %s (duration=%v)", table.Name, time.Since(startTime))
	return nil
}

func nextChunkPosition(pos []interface{}) []interface{} {
	result := make([]interface{}, len(pos))
	copy(result, pos)
	inc, err := increment(result[len(result)-1])
	if err != nil {
		panic(err)
	}
	result[len(result)-1] = inc
	return result
}

func increment(value interface{}) (interface{}, error) {
	switch value := value.(type) {
	case int64:
		return value + 1, nil
	default:
		return 0, errors.Errorf("can't (yet?) increment %v: %v", reflect.TypeOf(value), value)
	}
}
