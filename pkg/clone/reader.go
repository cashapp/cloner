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
func ReadTables(ctx context.Context, chunkerConn *sql.Conn, tableCh chan *Table, cmd *Clone, writeRequests chan Batch, writesInFlight *sync.WaitGroup, diffRequests chan DiffRequest) error {
	for {
		select {
		case table, more := <-tableCh:
			if !more {
				return nil
			}
			err := readTable(ctx, chunkerConn, table, cmd, writeRequests, writesInFlight, diffRequests)
			if err != nil {
				return errors.WithStack(err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

// readTables generates write batches for one table
func readTable(ctx context.Context, chunkerConn *sql.Conn, table *Table, cmd *Clone, writeRequests chan Batch, writesInFlight *sync.WaitGroup, diffRequests chan DiffRequest) error {
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
			// This channel can fill up so we check if the context is cancelled before we enqueue so we don't block
			select {
			case <-ctx.Done():
				return nil
			default:
			}
			done.Add(1)
			diffRequests <- DiffRequest{chunk, diffs, done}
		}
		// TODO this is a smell that we have to do a context friendly WaitGroup wait here...
		WaitGroupWait(ctx, done)
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		// All diffing done, close the diffs channel
		close(diffs)
		return nil
	})
	g.Go(func() error {
		return BatchTableWrites(ctx, cmd.WriteBatchSize, diffs, writeRequests, writesInFlight)
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
		close(ch)
	}()
	select {
	case <-ctx.Done():
	case <-ch:
	}
}
