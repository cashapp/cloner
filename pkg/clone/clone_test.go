package clone

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/vt/key"
)

func insertBunchaData(config DBConfig, namePrefix string, rowCount int) error {
	err := deleteAllData(config)
	if err != nil {
		return errors.WithStack(err)
	}
	db, err := config.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	for i := 0; i < rowCount; i++ {
		_, err = db.Exec(`
		INSERT INTO customers (name) VALUES (?)
	`, fmt.Sprintf("%s %d", namePrefix, i))
	}
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func countRows(target DBConfig, tableName string) (int, error) {
	db, err := target.DB()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	row := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+tableName)
	var rowCount int
	err = row.Scan(&rowCount)
	return rowCount, err
}

func countRowsShardFilter(target DBConfig, shard string) (int, error) {
	db, err := target.DB()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	spec, err := key.ParseShardingSpec(shard)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	rows, err := conn.QueryContext(ctx, "SELECT id FROM customers")
	if err != nil {
		return 0, errors.WithStack(err)
	}
	var rowCount int
	for rows.Next() {
		var id int64
		err = rows.Scan(&id)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		if InShard(uint64(id), spec) {
			rowCount++
		}
	}
	return rowCount, err
}

func TestCloneWithTargetData(t *testing.T) {
	source := vitessContainer.Config()
	target := tidbContainer.Config()

	rowCount := 1000
	err := insertBunchaData(source, "Name", rowCount)
	assert.NoError(t, err)

	// Insert some stuff that matches
	err = insertBunchaData(target, "Name", 50)
	assert.NoError(t, err)
	// Insert some stuff that DOES NOT match to trigger updates
	err = insertBunchaData(target, "AnotherName", 50)
	assert.NoError(t, err)
	// The clone should not touch the rows in the right 80- shard
	rightRowCountBefore, err := countRowsShardFilter(target, "80-")
	assert.NoError(t, err)

	clone := &Clone{
		Consistent:     false,
		QueueSize:      1000,
		ChunkSize:      5,
		WriteBatchSize: 5,
		ChunkerCount:   1,
		ReaderCount:    1,
		WriterCount:    1,
		ReadTimeout:    1 * time.Minute,
		WriteTimeout:   1 * time.Minute,
	}
	source.Database = "customer/-80@replica"
	err = clone.Run(Globals{
		Source: source,
		Target: target,
	})
	assert.NoError(t, err)

	// Sanity check the number of rows
	sourceRowCount, err := countRows(source, "customers")
	targetRowCount, err := countRowsShardFilter(target, "-80")
	assert.NoError(t, err)
	assert.Equal(t, sourceRowCount, targetRowCount)

	// Check we didn't delete the rows in the right shard in the target
	rightRowCountAfter, err := countRowsShardFilter(target, "80-")
	assert.Equal(t, rightRowCountBefore, rightRowCountAfter)

	// Do a full checksum
	checksum := &Checksum{
		QueueSize:    1000,
		ChunkSize:    5,
		ChunkerCount: 1,
		ReaderCount:  1,
		ReadTimeout:  1 * time.Minute,
	}
	diffs, err := checksum.run(Globals{
		Source: source,
		Target: target,
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(diffs))
}

// TestCloneWithNullBoolColumn reproduces a bug we found in production
func TestCloneWithNullBoolColumn(t *testing.T) {
	source := vitessContainer.Config()
	target := tidbContainer.Config()

	sourceDB, err := source.DB()
	assert.NoError(t, err)

	_, err = sourceDB.Exec(`
		INSERT INTO customer_passcodes (
			id,
			customer_id,
			original_customer_id,
			token,
			fidelius_token,
			active,
			unlinked_at,
			version,
			created_at,
			updated_at,
			last_verified_at 
		) VALUES (
			100020406,
			30027935561,
			30027935561,
			'P_5hpl5pcdquw95xr34ixy4b7ep',
			'fid-1-0dbd273bb139f524b524e95b8711550ccc78e3558d52c3c3e26c093842543771',
			NULL,
			'2020-10-22 14:33:16',
			18,
			'2020-08-24 13:29:44',
			'2020-10-22 14:33:16',
			'2020-10-22 14:33:16'
		)
	`)
	assert.NoError(t, err)

	targetDB, err := target.DB()
	assert.NoError(t, err)
	_, err = targetDB.Exec(`
		INSERT INTO customer_passcodes (
			id,
			customer_id,
			original_customer_id,
			token,
			fidelius_token,
			active,
			unlinked_at,
			version,
			created_at,
			updated_at,
			last_verified_at 
		) VALUES (
			100020406,
			30027935561,
			30027935561,
			'P_5hpl5pcdquw95xr34ixy4b7ep',
			'fid-1-0dbd273bb139f524b524e95b8711550ccc78e3558d52c3c3e26c093842543771',
			1,
			NULL,
			18,
			'2020-08-24 13:29:44',
			'2020-10-22 14:33:16',
			'2020-10-22 14:33:16'
		)
	`)
	assert.NoError(t, err)

	source.Database = "customer/-80@replica"
	sourceDB, err = source.DB()
	assert.NoError(t, err)
	rows, err := countRows(source, "customer_passcodes")
	// Make sure the row ended up in the right shard
	assert.Equal(t, 1, rows)

	clone := &Clone{
		Consistent:     false,
		QueueSize:      1000,
		ChunkSize:      5,
		WriteBatchSize: 5,
		ChunkerCount:   1,
		ReaderCount:    1,
		WriterCount:    1,
	}
	err = clone.Run(Globals{
		Source: source,
		Target: target,
	})
	assert.NoError(t, err)

	// Do a full checksum
	checksum := &Checksum{
		QueueSize:    1000,
		ChunkSize:    5,
		ChunkerCount: 1,
		ReaderCount:  1,
		ReadTimeout:  1 * time.Minute,
	}
	diffs, err := checksum.run(Globals{
		Source: source,
		Target: target,
	})
	assert.NoError(t, err)
	assert.Equal(t, 0, len(diffs))
}
