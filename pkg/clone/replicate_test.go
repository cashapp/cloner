package clone

import (
	"context"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
)

func TestReplicate(t *testing.T) {
	err := startAll()
	assert.NoError(t, err)

	rowCount := 1000
	err = insertBunchaData(vitessContainer.Config(), "Name", rowCount)
	assert.NoError(t, err)

	err = deleteAllData(tidbContainer.Config())
	assert.NoError(t, err)

	source := vitessContainer.Config()
	// We connect directly to the underlying MySQL database as vtgate does not support binlog streaming
	sourceDirect := DBConfig{
		Type:     MySQL,
		Host:     "localhost:" + vitessContainer.resource.GetPort("15002/tcp"),
		Username: "vt_dba",
		Password: "",
		Database: "vt_customer_-80",
	}

	target := tidbContainer.Config()

	readerConfig := ReaderConfig{
		SourceTargetConfig: SourceTargetConfig{
			Source: sourceDirect,
			Target: target,
		},
		ChunkSize: 5, // Smaller chunk size to make sure we're exercising chunking
		Config: Config{
			Tables: map[string]TableConfig{
				"customers": {
					// equivalent to -80
					TargetWhere:    "(vitess_hash(id) >> 56) < 128",
					WriteBatchSize: 5, // Smaller batch size to make sure we're exercising batching
				},
			},
		},
	}
	replicate := &Replicate{
		WriterConfig{
			ReaderConfig:            readerConfig,
			WriteBatchStatementSize: 3, // Smaller batch size to make sure we're exercising batching
		},
	}
	err = kong.ApplyDefaults(replicate)
	assert.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	err = deleteAllData(source)
	assert.NoError(t, err)

	// Run replication in separate thread
	g.Go(func() error {
		err := replicate.run(ctx)
		return err
	})

	// Write rows in a separate thread
	g.Go(func() error {
		db, err := source.DB()
		if err != nil {
			return errors.WithStack(err)
		}
		for i := 0; ; i++ {
			// Insert a new row
			_, err = db.ExecContext(ctx, `
				INSERT INTO customers (name) VALUES (CONCAT('New customer ', LEFT(MD5(RAND()), 8)))
			`)
			if err != nil {
				return errors.WithStack(err)
			}

			// Randomly update a row
			var randomCustomerId int64
			row := db.QueryRowContext(ctx, `SELECT id FROM customers ORDER BY rand() LIMIT 1`)
			err := row.Scan(&randomCustomerId)
			if err != nil {
				return errors.WithStack(err)
			}

			// Every five iterations randomly delete a row
			if i%5 == 0 {
				_, err = db.ExecContext(ctx, `DELETE FROM customers WHERE id = ?`, randomCustomerId)
				if err != nil {
					return errors.WithStack(err)
				}
			} else {
				// Otherwise update it
				_, err = db.ExecContext(ctx, ` 
					UPDATE customers SET name = CONCAT('Updated customer ', LEFT(MD5(RAND()), 8)) 
					WHERE id = ?
				`, randomCustomerId)
				if err != nil {
					return errors.WithStack(err)
				}
			}

			// Sleep a bit to make sure the replicator can keep up
			time.Sleep(100 * time.Millisecond)

			// Check if we were cancelled
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	})

	// Wait for a little bit to let the replicator exercise
	time.Sleep(10 * time.Second)

	// Do a full checksum
	checksum := &Checksum{
		ReaderConfig: readerConfig,
	}
	err = kong.ApplyDefaults(checksum)
	// If a chunk fails it might be because the replication is behind so we retry a bunch of times
	// we should eventually catch the chunk while replication is caught up
	checksum.FailedChunkRetryCount = 20
	assert.NoError(t, err)
	diffs, err := checksum.run(ctx)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(diffs))

	cancel()
	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		return
	}
	if err.Error() == "dial tcp: operation was canceled" {
		return
	}
	if strings.Contains(err.Error(), "context canceled") {
		return
	}
	assert.NoError(t, err)
}
