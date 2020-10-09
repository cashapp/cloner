package clone

import (
	"context"
	"database/sql"
)

// OpenSyncedConnections opens count connections that have a synchronized view of the database
func OpenSyncedConnections(ctx context.Context, db *sql.DB, count int) ([]*sql.Conn, error) {
	// TODO
	return nil, nil
}

// OpenConnections opens count connections
func OpenConnections(ctx context.Context, db *sql.DB, count int) ([]*sql.Conn, error) {
	// TODO
	return nil, nil
}

// OpenSyncedConnections opens count connections
func CloseConnections(conns []*sql.Conn) {
	for _, conn := range conns {
		_ = conn.Close()
	}
}
