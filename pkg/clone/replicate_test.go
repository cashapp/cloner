package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/mightyguava/autotx"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const heartbeatFrequency = 100 * time.Millisecond

var rollbackErr = fmt.Errorf("expected rollback")

func TestReplicateSingleThreaded(t *testing.T) {
	doTestReplicate(t, func(replicate *Replicate) {
		replicate.ReplicationParallelism = 1
	})
}

func TestReplicateParallel(t *testing.T) {
	doTestReplicate(t, func(replicate *Replicate) {
		replicate.ParallelTransactionBatchTimeout = 5 * heartbeatFrequency
		replicate.ParallelTransactionBatchMaxSize = 50
		replicate.ReplicationParallelism = 10
	})
}

func doTestReplicate(t *testing.T, replicateConfig func(*Replicate)) {
	err := startAll()
	require.NoError(t, err)

	rowCount := 5000
	err = insertBunchaData(context.Background(), vitessContainer.Config(), rowCount)
	require.NoError(t, err)

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
		ReadRetries: 1,
		ChunkSize:   5, // Smaller chunk size to make sure we're exercising chunking
		Config: Config{
			Tables: map[string]TableConfig{
				"customers": {
					// equivalent to -80
					TargetWhere:    "(vitess_hash(id) >> 56) < 128",
					WriteBatchSize: 5, // Smaller batch size to make sure we're exercising batching
				},
				"transactions": {
					// equivalent to -80
					TargetWhere:    "(vitess_hash(customer_id) >> 56) < 128",
					WriteBatchSize: 5, // Smaller batch size to make sure we're exercising batching
					KeyColumns:     []string{"customer_id", "id"},
				},
			},
		},
		UseConcurrencyLimits: false,
	}
	replicate := &Replicate{
		WriterConfig: WriterConfig{
			WriteRetries:            1,
			ReaderConfig:            readerConfig,
			WriteBatchStatementSize: 3, // Smaller batch size to make sure we're exercising batching
		},
		TaskName:               "customer/-80",
		HeartbeatFrequency:     heartbeatFrequency,
		CreateTables:           true,
		ReplicationParallelism: 10,
	}
	replicateConfig(replicate)
	err = kong.ApplyDefaults(replicate)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	err = deleteAllData(source)
	require.NoError(t, err)

	doWrite := atomic.NewBool(true)

	targetDB, err := target.DB()
	require.NoError(t, err)

	// Write rows in a separate thread
	writerCount := 5
	writerDelay := 500 * time.Millisecond
	for i := 0; i < writerCount; i++ {
		time.Sleep(writerDelay / 2)
		g.Go(func() error {
			db, err := source.DB()
			if err != nil {
				return errors.WithStack(err)
			}
			for {
				if doWrite.Load() {
					err := write(ctx, db)
					if err != nil && err != rollbackErr {
						return errors.WithStack(err)
					}
				}

				// Sleep a bit to make sure the replicator can keep up
				time.Sleep(writerDelay)

				// Check if we were cancelled
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
				}
			}
		})
	}

	// We start writing first then wait for a bit to start replicating so that we can exercise snapshotting
	time.Sleep(5 * time.Second)

	replicator, err := NewReplicator(*replicate)
	require.NoError(t, err)

	// Run replication in separate thread
	firstReplicationCtx, cancelFirstReplication := context.WithCancel(ctx)
	defer cancelFirstReplication()
	g.Go(func() error {
		err := replicator.run(firstReplicationCtx)
		if isCancelledError(err) {
			return nil
		}
		require.NoError(t, err)
		return err
	})

	// Wait for a little bit to let the replicator exercise
	time.Sleep(5 * time.Second)

	// Now do the snapshot (runs in the background)
	g.Go(func() error {
		return replicator.snapshot(ctx)
	})

	// Wait for the snapshot to complete
	time.Sleep(5 * time.Second)

	// Stop writing and make sure replication lag drops to heartbeat frequency
	err = waitFor(ctx, someReplicationLag(ctx, targetDB))
	require.NoError(t, err)
	doWrite.Store(false)
	err = waitFor(ctx, littleReplicationLag(ctx, targetDB))
	require.NoError(t, err)

	// Restart the writes
	doWrite.Store(true)

	// "Forcefully" kill the replicator
	cancelFirstReplication()

	// Wait for a little bit to let some replication lag build up then restart the replicator
	time.Sleep(5 * time.Second)
	g.Go(func() error {
		err := replicator.run(ctx)
		if isCancelledError(err) {
			return nil
		}
		return err
	})
	err = waitFor(ctx, someReplicationLag(ctx, targetDB))
	require.NoError(t, err)

	// Let replication catch up and then do a full checksum while replication is running
	time.Sleep(5 * time.Second)
	err = waitFor(ctx, littleReplicationLag(ctx, targetDB))
	require.NoError(t, err)
	checksum := &Checksum{
		ReaderConfig: readerConfig,
	}
	err = kong.ApplyDefaults(checksum)
	// If a chunk fails it might be because the replication is behind so we retry a bunch of times
	// we should eventually catch the chunk while replication is caught up
	checksum.FailedChunkRetryCount = 10
	require.NoError(t, err)
	diffs, err := checksum.run(ctx)
	require.NoError(t, err)

	cancel()
	err = g.Wait()
	if !isCancelledError(err) {
		require.NoError(t, err)
	}

	if len(diffs) > 0 {
		err := reportDiffs(diffs)
		assert.NoError(t, err)
		assert.Fail(t, "there were diffs (see above)")
	}
}

func reportDiffs(diffs []Diff) error {
	for _, diff := range diffs {
		should, err := rowToString(diff.Row.Data)
		if err != nil {
			return errors.WithStack(err)
		}
		var actual string
		if diff.Target != nil {
			actual, err = rowToString(diff.Target.Data)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		fmt.Printf("diff %v %v id=%v should=%s actual=%s\n",
			diff.Row.Table.Name, diff.Type, diff.Row.ID, should, actual)
	}
	return nil
}

func rowToString(data []interface{}) (string, error) {
	var result strings.Builder
	for i, datum := range data {
		s, err := coerceString(datum)
		if err != nil {
			return "", errors.WithStack(err)
		}
		result.WriteString(s)
		if i < len(data)-1 {
			result.WriteString(",")
		}
	}
	return result.String(), nil
}

func isCancelledError(err error) bool {
	return errors.Is(err, context.Canceled) ||
		err.Error() == "dial tcp: operation was canceled" ||
		strings.Contains(err.Error(), "context canceled")
}

func someReplicationLag(ctx context.Context, db *sql.DB) func() error {
	return func() error {
		lag, _, err := readReplicationLag(ctx, db)
		if err != nil {
			return errors.WithStack(err)
		}
		if lag <= 0 {
			return errors.Errorf("no replication lag")
		}
		return nil
	}
}

func littleReplicationLag(ctx context.Context, db *sql.DB) func() error {
	return func() error {
		expectedLag := time.Duration(1.5 * float64(heartbeatFrequency))
		reads := testutil.ToFloat64(heartbeatsRead)
		if reads == 0 {
			return errors.Errorf("we haven't read any heartbeats yet")
		}
		lag, _, err := readReplicationLag(ctx, db)
		if err != nil {
			return errors.WithStack(err)
		}
		if lag > expectedLag {
			return errors.Errorf("replication lag did not drop yet, it's still %v", lag)
		}
		return nil
	}
}

func readReplicationLag(ctx context.Context, db *sql.DB) (time.Duration, time.Time, error) {
	// TODO retries with backoff?
	stmt := fmt.Sprintf("SELECT time FROM `%s` WHERE name = ?", "_cloner_heartbeat")
	row := db.QueryRowContext(ctx, stmt, "customer/-80")
	var lastHeartbeat time.Time
	err := row.Scan(&lastHeartbeat)
	if err != nil {
		return 0, time.Time{}, errors.WithStack(err)
	}
	lag := time.Now().UTC().Sub(lastHeartbeat)
	return lag, lastHeartbeat, nil
}

func waitFor(ctx context.Context, condition func() error) error {
	ctx, cancel := context.WithTimeout(ctx, 180*time.Second)
	defer cancel()
	return backoff.Retry(condition, backoff.WithContext(backoff.NewConstantBackOff(heartbeatFrequency), ctx))
}

func write(ctx context.Context, db *sql.DB) (err error) {
	return autotx.Transact(ctx, db, func(tx *sql.Tx) error {
		// Insert a new row
		_, err = tx.ExecContext(ctx, `
				INSERT INTO customers (name) VALUES (CONCAT('New customer ', LEFT(MD5(RAND()), 8)))
			`)
		if err != nil {
			return errors.WithStack(err)
		}

		var randomCustomerId int64
		row := tx.QueryRowContext(ctx, `SELECT id FROM customers ORDER BY rand() LIMIT 1`)
		err = row.Scan(&randomCustomerId)
		if err != nil {
			return errors.WithStack(err)
		}

		// Sometimes we randomly update as well to exercise multiple updates in the same transaction
		doMultipleUpdates := rand.Intn(25) == 0

		// Insert a new row
		insterted, err := tx.ExecContext(ctx, `
				INSERT INTO transactions (customer_id, amount_cents, description) 
				VALUES (?, RAND()*9999+1, CONCAT('Description ', LEFT(MD5(RAND()), 8)))
			`, randomCustomerId)
		if err != nil {
			return errors.WithStack(err)
		}

		if doMultipleUpdates {
			transactionId, err := insterted.LastInsertId()
			if err != nil {
				return errors.WithStack(err)
			}
			// and update it immediately to exercise insert and update in the same transaction
			_, err = tx.ExecContext(ctx, `
				UPDATE transactions SET description = CONCAT('Description ', LEFT(MD5(RAND()), 8)) WHERE id = ?				
			`, transactionId)
			if err != nil {
				return errors.WithStack(err)
			}
		}

		// Do some random updates
		for i := 0; i < 2; i++ {
			// Randomly update or delete rows
			var randomCustomerId int64
			row := tx.QueryRowContext(ctx, `SELECT id FROM customers ORDER BY rand() LIMIT 1`)
			err = row.Scan(&randomCustomerId)
			if err != nil {
				return errors.WithStack(err)
			}

			// For every five inserted rows we randomly delete one
			doDelete := rand.Intn(25) == 0

			if doDelete {
				_, err = tx.ExecContext(ctx, `DELETE FROM customers WHERE id = ?`, randomCustomerId)
				if err != nil {
					return errors.WithStack(err)
				}
			} else {
				// Otherwise update it, several times to exercise multiple updates in a single transaction
				_, err = tx.ExecContext(ctx, ` 
					UPDATE customers SET name = CONCAT('Updated customer ', LEFT(MD5(RAND()), 8)) 
					WHERE id = ?
				`, randomCustomerId)
				if err != nil {
					return errors.WithStack(err)
				}
				if doMultipleUpdates {
					_, err = tx.ExecContext(ctx, ` 
						UPDATE customers SET name = CONCAT('Updated customer ', LEFT(MD5(RAND()), 8)) 
						WHERE id = ?
					`, randomCustomerId)
					if err != nil {
						return errors.WithStack(err)
					}
				}
			}
		}

		// Roll back a few transactions
		doRollback := rand.Intn(20) == 0
		if doRollback {
			return rollbackErr
		}

		return nil
	})
}
