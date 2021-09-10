package clone

import (
	"context"
	"fmt"
	"testing"
	"vitess.io/vitess/go/vt/proto/topodata"

	"github.com/alecthomas/kong"
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
		if inShard(uint64(id), spec) {
			rowCount++
		}
	}
	return rowCount, err
}

func inShard(id uint64, shard []*topodata.KeyRange) bool {
	destination := key.DestinationKeyspaceID(vhash(id))
	for _, keyRange := range shard {
		if key.KeyRangeContains(keyRange, destination) {
			return true
		}
	}
	return false
}

func TestShardedCloneWithTargetData(t *testing.T) {
	err := startAll()
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()

	rowCount := 1000
	err = insertBunchaData(source, "Name", rowCount)
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

	source.Database = "customer/-80@replica"
	readerConfig := ReaderConfig{
		SourceTargetConfig: SourceTargetConfig{
			Source: source,
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
	clone := &Clone{
		WriterConfig{
			ReaderConfig:            readerConfig,
			WriteBatchStatementSize: 3, // Smaller batch size to make sure we're exercising batching
		},
	}
	err = kong.ApplyDefaults(clone)
	// Turn on CRC32 checksum, it works on shard targeted clones from Vitess!
	clone.UseCRC32Checksum = true
	assert.NoError(t, err)
	err = clone.Run()
	assert.NoError(t, err)

	// Sanity check the number of rows
	sourceRowCount, err := countRows(source, "customers")
	assert.NoError(t, err)
	targetRowCount, err := countRowsShardFilter(target, "-80")
	assert.NoError(t, err)
	assert.Equal(t, sourceRowCount, targetRowCount)

	// Check we didn't delete the rows in the right shard in the target
	rightRowCountAfter, err := countRowsShardFilter(target, "80-")
	assert.NoError(t, err)
	assert.Equal(t, rightRowCountBefore, rightRowCountAfter)

	// Do a full checksum
	checksum := &Checksum{
		ReaderConfig: readerConfig,
	}
	err = kong.ApplyDefaults(checksum)
	// Turn on CRC32 checksum, it works on shard targeted clones from Vitess!
	clone.UseCRC32Checksum = true
	assert.NoError(t, err)
	diffs, err := checksum.run(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, len(diffs))
}

func TestUnshardedClone(t *testing.T) {
	t.Skip("unsharded vitess as a source is not currently supported")

	err := startAll()
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()

	rowCount := 1000
	err = insertBunchaData(source, "Name", rowCount)
	assert.NoError(t, err)

	// Insert some stuff that matches
	err = insertBunchaData(target, "Name", 50)
	assert.NoError(t, err)
	// Insert some stuff that DOES NOT match to trigger updates
	err = insertBunchaData(target, "AnotherName", 50)
	assert.NoError(t, err)

	source.Database = "@replica"
	clone := &Clone{
		WriterConfig{
			ReaderConfig: ReaderConfig{
				SourceTargetConfig: SourceTargetConfig{
					Source: source,
					Target: target,
				},
				ChunkSize:      5, // Smaller chunk size to make sure we're exercising chunking
				WriteBatchSize: 5, // Smaller batch size to make sure we're exercising batching
			},
			WriteBatchStatementSize: 3, // Smaller batch size to make sure we're exercising batching
		},
	}
	err = kong.ApplyDefaults(clone)
	assert.NoError(t, err)
	err = clone.Run()
	assert.NoError(t, err)

	// Do a full checksum
	checksum := &Checksum{
		ReaderConfig: ReaderConfig{
			SourceTargetConfig: SourceTargetConfig{
				Source: source,
				Target: target,
			},
		},
	}
	err = kong.ApplyDefaults(checksum)
	assert.NoError(t, err)
	diffs, err := checksum.run(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, len(diffs))
}

func TestCloneNoDiff(t *testing.T) {
	err := startAll()
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()

	rowCount := 1000
	err = insertBunchaData(source, "Name", rowCount)
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

	source.Database = "customer/-80@replica"
	readerConfig := ReaderConfig{
		SourceTargetConfig: SourceTargetConfig{
			Source: source,
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
	clone := &Clone{
		WriterConfig{
			ReaderConfig:            readerConfig,
			WriteBatchStatementSize: 3, // Smaller batch size to make sure we're exercising batching
			NoDiff:                  true,
		},
	}
	err = kong.ApplyDefaults(clone)
	assert.NoError(t, err)
	err = clone.Run()
	assert.NoError(t, err)

	// Sanity check the number of rows
	sourceRowCount, err := countRows(source, "customers")
	assert.NoError(t, err)
	targetRowCount, err := countRowsShardFilter(target, "-80")
	assert.NoError(t, err)
	assert.Equal(t, sourceRowCount, targetRowCount)

	// Check we didn't delete the rows in the right shard in the target
	rightRowCountAfter, err := countRowsShardFilter(target, "80-")
	assert.NoError(t, err)
	assert.Equal(t, rightRowCountBefore, rightRowCountAfter)

	// Do a full checksum
	checksum := &Checksum{
		ReaderConfig: readerConfig,
	}
	err = kong.ApplyDefaults(checksum)
	assert.NoError(t, err)
	diffs, err := checksum.run(context.Background())
	assert.NoError(t, err)
	// Nothing is deleted so some stuff will be left around
	assert.Equal(t, 27, len(diffs))
}
