package clone

import (
	"context"
	"database/sql"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// ReadTables generates batches for each table
func ReadTables(
	ctx context.Context,
	chunkerConn *sql.Conn,
	tableCh chan *Table,
	cmd *Clone,
	batches chan Batch,
	diffRequests chan DiffRequest,
) error {
	for {
		select {
		case table, more := <-tableCh:
			if !more {
				return nil
			}
			err := readTable(ctx, chunkerConn, table, cmd, batches, diffRequests)
			if err != nil {
				return errors.WithStack(err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// readTables generates write batches for one table
func readTable(
	ctx context.Context,
	chunkerConn *sql.Conn,
	table *Table,
	cmd *Clone,
	batches chan Batch,
	diffRequests chan DiffRequest,
) error {
	logger := log.WithField("task", "reader").WithField("table", table.Name)
	logger.Infof("start")
	defer logger.Infof("done")

	chunks := make(chan Chunk, cmd.QueueSize)
	diffs := make(chan Diff, cmd.QueueSize)

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		err := generateTableChunks(ctx, chunkerConn, table, cmd.ChunkSize, chunks)
		close(chunks)
		return err
	})
	// Request diffing for every chunk
	g.Go(func() error {
		done := &sync.WaitGroup{}
		for chunk := range chunks {
			done.Add(1)
			diffRequests <- DiffRequest{chunk, diffs, done}
		}
		WaitGroupWait(ctx, done)
		// All diffing done, close the diffs channel
		close(diffs)
		return nil
	})
	g.Go(func() error {
		return BatchTableWrites(ctx, cmd.WriteBatchSize, diffs, batches)
	})

	if err := g.Wait(); err != nil {
		logger.WithError(err).Errorf("%v", err)
		return err
	}

	return nil
}

// WaitGroupWait is a context friendly wait on a WaitGroup
func WaitGroupWait(ctx context.Context, wg *sync.WaitGroup) {
	ch := make(chan struct{})
	go func() {
		wg.Wait()
		ch <- struct{}{}
	}()
	select {
	case <-ctx.Done():
	case <-ch:
	}
}
