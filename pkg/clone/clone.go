package clone

import (
	"context"
	"database/sql"
	"sync"
)

// Clone applies the necessary changes to target to make it look like source
func Clone(config *Config, source *sql.DB, target *sql.DB) error {
	// TODO timeout?
	ctx, cancel := context.WithCancel(context.Background())

	// Create synced source and chunker conns
	sourceConns, err := OpenSyncedConnections(ctx, source, config.ChunkerCount+config.ReaderCount)
	if err != nil {
		return err
	}
	defer CloseConnections(sourceConns)

	chunkerConns := sourceConns[:config.ChunkerCount]
	sourceConns = sourceConns[config.ChunkerCount:]

	// Create synced target reader conns
	targetConns, err := OpenConnections(ctx, target, config.ReaderCount)
	if err != nil {
		return err
	}
	defer CloseConnections(targetConns)

	// Create writer connections
	writerConns, err := OpenConnections(ctx, target, config.WriterCount)
	if err != nil {
		return err
	}
	defer CloseConnections(writerConns)

	// Load tables
	tables, err := LoadTables(chunkerConns[0])
	if err != nil {
		return err
	}

	chunks := make(chan Chunk, config.QueueSize)
	diffs := make(chan Diff, config.QueueSize)
	batches := make(chan Batch, config.QueueSize)
	errors := make(chan error)
	wg := &sync.WaitGroup{}

	// Generate chunks of all source tables
	GenerateChunks(ctx, chunkerConns, tables, chunks, cancel, errors)

	// Batch writes
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := BatchWrites(ctx, config.WriteBatchSize, diffs, batches)
		if err != nil {
			errors <- err
			cancel()
		}
	}()

	// Write batches
	for i, _ := range writerConns {
		conn := writerConns[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := Write(ctx, conn, batches)
			if err != nil {
				errors <- err
				cancel()
			}
		}()
	}

	// Diff chunks
	for i, _ := range sourceConns {
		fromConn := sourceConns[i]
		toConn := targetConns[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := DiffChunks(ctx, fromConn, toConn, chunks, diffs)
			if err != nil {
				errors <- err
				cancel()
			}
		}()
	}

	// Wait for everything to complete
	wg.Wait()
	cancel()
	close(errors)

	// Check for errors
	select {
	case err := <-errors:
		// Return the first error only, ignore the rest (they should have logged)
		return err
	default:
		return nil
	}
}
