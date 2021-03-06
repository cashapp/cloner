package clone

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
)

// OpenSyncedConnections opens count connections that have a synchronized view of the database
func OpenSyncedConnections(ctx context.Context, source *sql.DB, count int) ([]*sql.Conn, error) {
	// Lock all the tables
	// Create connections
	// Check the current GTID (using one of those connections)
	// Unlock the tables
	// Return the connections and the GTID they are synced to

	// TODO
	return nil, errors.Errorf("consistent clone not implemented yet")
}

// OpenConnections opens count connections
func OpenConnections(ctx context.Context, db *sql.DB, count int) ([]*sql.Conn, error) {
	var err error
	conns := make([]*sql.Conn, count)
	for i := range conns {
		conns[i], err = db.Conn(ctx)
	}
	return conns, err
}

func CloseConnections(conns []*sql.Conn) {
	for _, conn := range conns {
		go conn.Close()
	}
}
