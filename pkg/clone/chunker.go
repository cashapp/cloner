package clone

import (
	"context"
	"database/sql"
	"sync"
)

// GenerateChunks generates chunks from all tables and then closes the channel
func GenerateChunks(ctx context.Context, conns []*sql.Conn, tables []*Table, chunks chan Chunk, cancel context.CancelFunc, errors chan error) {
	wg := &sync.WaitGroup{}
	tableChan := make(chan *Table)
	for i, _ := range conns {
		conn := conns[i]
		go func() {
			defer wg.Done()
			err := GenerateTableChunks(ctx, conn, tableChan, chunks)
			if err != nil {
				errors <- err
				cancel()
			}
		}()
	}
	for _, table := range tables {
		tableChan <- table
	}
	wg.Wait()
	close(chunks)
}

type Chunk struct {
	Table *Table
	Start *int64 // nil if first chunk
	End   *int64 // nil if last chunk
}

func GenerateTableChunks(ctx context.Context, conn *sql.Conn, tables chan *Table, chunks chan Chunk) error {
	for {
		select {
		case _ /*table*/, more := <-tables:
			if more {
				// TODO generate chunks for the table
				/*
				   	    select
				              min(tmp.id) as start_rowid,
				              max(tmp.id) as end_rowid,
				              count(*) as page_size
				          from (
				              select
				                  id,
				                  row_number() over (order by id) as row_num
				              from {0}
				          ) tmp
				          group by floor((tmp.row_num - 1) / ?)
				          order by start_rowid

				*/
			} else {
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}
