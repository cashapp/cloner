package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-mysql-org/go-mysql/schema"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const heartbeatFrequency = 100 * time.Millisecond

// TODO test for rollback

func TestReplicate(t *testing.T) {
	err := startAll()
	require.NoError(t, err)

	rowCount := 1000
	err = insertBunchaData(vitessContainer.Config(), "Name", rowCount)
	require.NoError(t, err)

	err = deleteAllData(tidbContainer.Config())
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
		TaskName:           "customer/-80",
		HeartbeatFrequency: heartbeatFrequency,
		CreateTables:       true,
	}
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
	g.Go(func() error {
		db, err := source.DB()
		if err != nil {
			return errors.WithStack(err)
		}
		for i := 0; ; i++ {
			if doWrite.Load() {
				err := write(ctx, db, i%5 == 0)
				if err != nil {
					return errors.WithStack(err)
				}
			}

			// Sleep a bit to make sure the replicator can keep up
			time.Sleep(50 * time.Millisecond)

			// Check if we were cancelled
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
	})

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
	checksum.FailedChunkRetryCount = 20
	require.NoError(t, err)
	diffs, err := checksum.run(ctx)
	require.NoError(t, err)

	cancel()
	err = g.Wait()
	if !isCancelledError(err) {
		require.NoError(t, err)
	}

	if len(diffs) > 0 {
		for _, diff := range diffs {
			fmt.Printf("diff %v id=%v should=%v actual=%v\n", diff.Type, diff.Row.ID, diff.Row, diff.Target)
		}
		assert.Fail(t, "there were diffs (see above)")
	}
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
			return errors.Errorf("replication lag did not drop yet")
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
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	return backoff.Retry(condition, backoff.WithContext(backoff.NewConstantBackOff(heartbeatFrequency), ctx))
}

func write(ctx context.Context, db *sql.DB, delete bool) (err error) {
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
	err = row.Scan(&randomCustomerId)
	if err != nil {
		return errors.WithStack(err)
	}

	// Every five iterations randomly delete a row
	if delete {
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
	return nil
}

func TestOngoingChunkReconcileBinlogEvents(t *testing.T) {
	tests := []struct {
		name         string
		chunk        testChunk
		start        int64
		end          int64
		eventType    MutationType
		startingRows [][]interface{}
		eventRows    [][]interface{}
		resultRows   [][]interface{}
	}{
		{
			name:  "delete outside chunk range",
			start: 5,  // inclusive
			end:   11, // exclusive
			startingRows: [][]interface{}{
				{5, "customer name"},
			},

			eventType: Delete,
			eventRows: [][]interface{}{
				{11, "other customer name"},
			},

			resultRows: [][]interface{}{
				{5, "customer name"},
			},
		},
		{
			name:  "delete inside chunk range",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{5, "customer name"},
			},

			eventType: Delete,
			eventRows: [][]interface{}{
				{5, "customer name"},
			},

			resultRows: [][]interface{}{},
		},
		{
			name:  "insert after",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{5, "customer name"},
			},

			eventType: Insert,
			eventRows: [][]interface{}{
				{6, "other customer name"},
			},

			resultRows: [][]interface{}{
				{5, "customer name"},
				{6, "other customer name"},
			},
		},
		{
			name:  "insert before",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{6, "customer name"},
			},

			eventType: Insert,
			eventRows: [][]interface{}{
				{5, "other customer name"},
			},

			resultRows: [][]interface{}{
				{5, "other customer name"},
				{6, "customer name"},
			},
		},
		{
			name:  "insert middle",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{5, "customer name #5"},
				{7, "customer name #7"},
			},

			eventType: Insert,
			eventRows: [][]interface{}{
				{6, "customer name #6"},
			},

			resultRows: [][]interface{}{
				{5, "customer name #5"},
				{6, "customer name #6"},
				{7, "customer name #7"},
			},
		},
		{
			name:  "multiple rows mixed insert and update",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{5, "customer name #5"},
				{7, "customer name #7"},
			},

			eventType: Insert,
			eventRows: [][]interface{}{
				{6, "customer name #6"},
				{7, "customer name #7 updated"},
			},

			resultRows: [][]interface{}{
				{5, "customer name #5"},
				{6, "customer name #6"},
				{7, "customer name #7 updated"},
			},
		},
		{
			name:  "update",
			start: 5,
			end:   11,
			startingRows: [][]interface{}{
				{6, "customer name"},
				{7, "other customer name"},
			},

			eventType: Update,
			eventRows: [][]interface{}{
				{6, "updated customer name"},
				{11, "outside of chunk range"},
			},

			resultRows: [][]interface{}{
				{6, "updated customer name"},
				{7, "other customer name"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tableName := "customer"
			tableSchema := &schema.Table{
				Name:      tableName,
				PKColumns: []int{0},
				Columns: []schema.TableColumn{
					{Name: "id"},
					{Name: "name"},
				},
			}
			table := &Table{
				Name:       tableName,
				MysqlTable: tableSchema,
			}
			inputRows := make([]*Row, len(test.startingRows))
			for i, row := range test.startingRows {
				inputRows[i] = &Row{
					Table: table,
					ID:    int64(row[0].(int)),
					Data:  row,
				}
			}
			chunk := &ChunkSnapshot{
				InsideWatermarks: true,
				Rows:             inputRows,
				Chunk: Chunk{
					Start: test.start,
					End:   test.end,
					Table: table,
				},
			}
			err := chunk.reconcileBinlogEvent(
				Mutation{
					Type:  test.eventType,
					Table: table,
					Rows:  test.eventRows,
				},
			)
			require.NoError(t, err)

			assert.Equal(t, len(test.resultRows), len(chunk.Rows))
			for i, expectedRow := range test.resultRows {
				actualRow := chunk.Rows[i]
				for j, cell := range expectedRow {
					assert.Equal(t, cell, actualRow.Data[j])
				}
			}
		})
	}
}
