package clone

import (
	"context"
	"database/sql"
	"fmt"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// GenerateChunks generates chunks from all tables and then closes the channel, blocks until done
func GenerateChunks(ctx context.Context, conns []*sql.Conn, tables []*Table, chunkSize int, chunks chan Chunk) error {
	tableChan := make(chan *Table, len(tables))
	for _, table := range tables {
		tableChan <- table
	}
	close(tableChan)

	wg := &sync.WaitGroup{}
	errCh := make(chan error)
	waitCh := make(chan interface{})
	go func() {
		wg.Wait()
		close(waitCh)
	}()
	for i, _ := range conns {
		conn := conns[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := GenerateTableChunks(ctx, conn, tableChan, chunkSize, chunks)
			if err != nil {
				log.Errorf("Chunker error: %v", err)
				errCh <- err
			}
		}()
	}
	select {
	case err := <-errCh:
		return err
	case <-waitCh:
		close(chunks)
		return nil
	}
}

type Chunk struct {
	Table *Table
	Start int64
	End   int64
	First bool // first chunk
	Last  bool
	Size  int
}

func GenerateTableChunks(ctx context.Context, conn *sql.Conn, tables chan *Table, chunkSize int, chunks chan Chunk) error {
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
//	fmt.Println(sql)
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

func generateTableChunks(ctx context.Context, conn *sql.Conn, table *Table, chunkSize int, chunks chan Chunk) error {
	rows, err := conn.QueryContext(ctx, fmt.Sprintf("select %s from  %s order by %s asc",
		table.IDColumn, table.Name, table.IDColumn))
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()
	currentChunkSize := 0
	first := true
	var startId int64
	startIdSet := false
	var id int64
	next := rows.Next()
	for next {
		err := rows.Scan(&id)
		if err != nil {
			return errors.WithStack(err)
		}

		currentChunkSize++

		if !startIdSet {
			startId = id
			startIdSet = true
		}

		next = rows.Next()

		if currentChunkSize == chunkSize {
			chunks <- Chunk{
				Table: table,
				Start: startId,
				End:   id,
				First: first,
				Last:  !next,
				Size:  currentChunkSize,
			}
			// Next id should be the next start id
			startIdSet = false
			// We are no longer the first chunk
			first = false
			// We have no rows in the next chunk yet
			currentChunkSize = 0
		}
	}
	// Send any partial chunk we might have
	if currentChunkSize > 0 {
		chunks <- Chunk{
			Table: table,
			Start: startId,
			End:   id,
			First: first,
			Last:  true,
			Size:  currentChunkSize,
		}
		first = false
	}
	return nil
}
