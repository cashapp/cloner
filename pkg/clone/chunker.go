package clone

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	chunksEnqueued = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chunks_enqueued",
			Help: "How many chunks has been enqueued, partitioned by table.",
		},
		[]string{"table"},
	)
)

// GenerateChunks generates chunks from all tables and then closes the channel, blocks until done
func GenerateChunks(ctx context.Context, conns []*sql.Conn, tables []*Table, chunkSize int, chunks chan Chunk) error {
	tableChan := make(chan *Table, len(tables))
	for _, table := range tables {
		tableChan <- table
	}
	close(tableChan)

	g, ctx := errgroup.WithContext(ctx)
	for i := range conns {
		conn := conns[i]
		g.Go(func() error {
			return GenerateTableChunks(ctx, conn, tableChan, chunkSize, chunks)
		})
	}
	return g.Wait()
}

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

func GenerateTableChunks(ctx context.Context, conn DBReader, tables chan *Table, chunkSize int, chunks chan Chunk) error {
	for {
		select {
		case table, more := <-tables:
			if more {
				err := generateTableChunks(ctx, conn, table, chunkSize, chunks)
				if err != nil {
					return errors.WithStack(err)
				}
			} else {
				log.Debugf("Chunker done!")
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

//func generateTableChunks(ctx context.Context, conn *sql.Conn, table *Table, chunkSize int, chunks chan Chunk) error {
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

func generateTableChunks(ctx context.Context, conn DBReader, table *Table, chunkSize int, chunks chan Chunk) error {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %s from %s order by %s asc",
		table.IDColumn, table.Name, table.IDColumn))
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()
	currentChunkSize := 0
	first := true
	startId := int64(0)
	var id int64
	next := rows.Next()
	for next {
		err := rows.Scan(&id)
		if err != nil {
			return errors.WithStack(err)
		}

		currentChunkSize++

		next = rows.Next()

		if currentChunkSize == chunkSize {
			chunksEnqueued.WithLabelValues(table.Name).Inc()
			chunks <- Chunk{
				Table: table,
				Start: startId,
				End:   id,
				First: first,
				Last:  !next,
				Size:  currentChunkSize,
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
		chunks <- Chunk{
			Table: table,
			Start: startId,
			End:   id,
			First: first,
			Last:  true,
			Size:  currentChunkSize,
		}
	}
	return nil
}
