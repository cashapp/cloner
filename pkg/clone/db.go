package clone

import (
	"context"
	"database/sql"
)

// DBReader is an interface that can be implemented by sql.Conn or sql.Tx or sql.DB so that we can
// easily change synchronization method
type DBReader interface {
	QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error)
}

type DBWriter interface {
	DBReader
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}
