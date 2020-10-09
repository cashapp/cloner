package clone

import (
	"context"
	"database/sql"
)

type Row struct {
	Table *Table
	ID    int64
	Data  []interface{}
}

type RowStream interface {
	// Next returns the next row or nil if we're done
	Next(context.Context) (*Row, error)
}

func StreamChunk(ctx context.Context, conn *sql.Conn, chunk Chunk) RowStream {
	// TODO
	return nil
}
