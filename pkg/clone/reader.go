package clone

import (
	"context"
	"database/sql"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"vitess.io/vitess/go/vt/proto/topodata"
)

// ReadTables generates batches for each table
func ReadTables(ctx context.Context, chunkerConn *sql.Conn, sourceConn *sql.Conn, targetConn *sql.Conn, shardingSpec []*topodata.KeyRange, tableCh chan *Table, chunkSize int, batchSize int, batches chan Batch) error {
	for {
		select {
		case table, more := <-tableCh:
			if !more {
				return nil
			}
			err := readTable(ctx, chunkerConn, sourceConn, targetConn, shardingSpec, table, chunkSize, batchSize, batches)
			if err != nil {
				return errors.WithStack(err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// readTables generates write batches for one table
func readTable(ctx context.Context, chunkerConn *sql.Conn, sourceConn *sql.Conn, targetConn *sql.Conn, shardingSpec []*topodata.KeyRange, table *Table, chunkSize int, batchSize int, batches chan Batch) error {
	logger := log.WithField("task", "reader").WithField("table", table.Name)
	logger.Infof("start")
	defer logger.Infof("done")

	chunks := make(chan Chunk)
	diffs := make(chan Diff)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		err := generateTableChunks(ctx, chunkerConn, table, chunkSize, chunks)
		close(chunks)
		return err
	})
	g.Go(func() error {
		err := DiffChunks(ctx, sourceConn, targetConn, shardingSpec, chunks, diffs)
		close(diffs)
		return err
	})
	g.Go(func() error {
		return BatchTableWrites(ctx, batchSize, diffs, batches)
	})

	if err := g.Wait(); err != nil {
		logger.WithError(err).Errorf("%v", err)
		return err
	}

	return nil
}
