package clone

import (
	"context"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
)

func TestOneShardCloneWithTargetData(t *testing.T) {
	_, _, err := startAll()
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()

	rowCount := 1000
	err = insertBunchaData(context.Background(), source, rowCount)
	assert.NoError(t, err)

	// Insert some stuff that matches
	err = insertBunchaData(context.Background(), target, 50)
	assert.NoError(t, err)
	// Insert some stuff that DOES NOT match to trigger updates
	err = insertBunchaData(context.Background(), target, 50)
	assert.NoError(t, err)
	// The clone should not touch the rows in the right 80- shard
	rightRowCountBefore, err := countRowsShardFilter(target, "customers", "80-")
	assert.NoError(t, err)

	source.Database = "customer/-80@replica"
	readerConfig := ReaderConfig{
		SourceTargetConfig: SourceTargetConfig{
			Source: source,
			Target: target,
		},
		ThroughputLoggingFrequency: 100 * time.Millisecond,
		ChunkSize:                  5, // Smaller chunk size to make sure we're exercising chunking
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
	}
	clone := &Clone{
		WriterConfig: WriterConfig{
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
	targetRowCount, err := countRowsShardFilter(target, "customers", "-80")
	assert.NoError(t, err)
	assert.Equal(t, sourceRowCount, targetRowCount)

	// Check we didn't delete the rows in the right shard in the target
	rightRowCountAfter, err := countRowsShardFilter(target, "customers", "80-")
	assert.NoError(t, err)
	assert.Equal(t, rightRowCountBefore, rightRowCountAfter)

	// Do a full checksum
	checksum := &Checksum{
		ReaderConfig: readerConfig,
	}
	err = kong.ApplyDefaults(checksum)
	checksum.IgnoreReplicationLag = true
	// Turn on CRC32 checksum, it works on shard targeted clones from Vitess!
	checksum.UseCRC32Checksum = true
	assert.NoError(t, err)
	diffs, err := checksum.run(context.Background())
	assert.NoError(t, err)
	err = reportDiffs(diffs)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(diffs))
}

func TestUnshardedClone(t *testing.T) {
	_, _, err := startAll()
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()

	rowCount := 1000
	err = insertBunchaData(context.Background(), source, rowCount)
	assert.NoError(t, err)

	// Insert some stuff that matches
	err = insertBunchaData(context.Background(), target, 50)
	assert.NoError(t, err)
	// Insert some stuff that DOES NOT match to trigger updates
	err = insertBunchaData(context.Background(), target, 50)
	assert.NoError(t, err)

	source.Database = "@replica"
	clone := &Clone{
		WriterConfig: WriterConfig{
			ReaderConfig: ReaderConfig{
				SourceTargetConfig: SourceTargetConfig{
					Source: source,
					Target: target,
				},
				ThroughputLoggingFrequency: 100 * time.Millisecond,
				ChunkSize:                  5, // Smaller chunk size to make sure we're exercising chunking
				WriteBatchSize:             5, // Smaller batch size to make sure we're exercising batching
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
	checksum.IgnoreReplicationLag = true
	assert.NoError(t, err)
	diffs, err := checksum.run(context.Background())
	assert.NoError(t, err)
	assert.Equal(t, 0, len(diffs))
}

func TestCloneNoDiff(t *testing.T) {
	_, _, err := startAll()
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()

	rowCount := 1000
	err = insertBunchaData(context.Background(), source, rowCount)
	assert.NoError(t, err)

	// Insert some stuff that matches
	err = insertBunchaData(context.Background(), target, 50)
	assert.NoError(t, err)
	// Insert some stuff that DOES NOT match to trigger updates
	err = insertBunchaData(context.Background(), target, 50)
	assert.NoError(t, err)
	// The clone should not touch the rows in the right 80- shard
	rightRowCountBefore, err := countRowsShardFilter(target, "customers", "80-")
	assert.NoError(t, err)

	sourceLeft := vitessContainer.Config()
	sourceLeft.Database = "customer/-80@master"
	readerConfig := ReaderConfig{
		SourceTargetConfig: SourceTargetConfig{
			Source: sourceLeft,
			Target: target,
		},
		ThroughputLoggingFrequency: 100 * time.Millisecond,
		ChunkSize:                  5, // Smaller chunk size to make sure we're exercising chunking
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
		WriterConfig: WriterConfig{
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
	sourceRowCount, err := countRows(sourceLeft, "customers")
	assert.NoError(t, err)
	targetRowCount, err := countRowsShardFilter(target, "customers", "-80")
	assert.NoError(t, err)
	assert.Equal(t, sourceRowCount, targetRowCount)

	// Check we didn't delete the rows in the right shard in the target
	rightRowCountAfter, err := countRowsShardFilter(target, "customers", "80-")
	assert.NoError(t, err)
	assert.Equal(t, rightRowCountBefore, rightRowCountAfter)

	// Do a full checksum
	checksum := &Checksum{
		ReaderConfig: readerConfig,
	}
	err = kong.ApplyDefaults(checksum)
	checksum.IgnoreReplicationLag = true
	assert.NoError(t, err)
	diffs, err := checksum.run(context.Background())
	assert.NoError(t, err)
	// Nothing is deleted so some stuff will be left around
	assert.Equal(t, 43, len(diffs))
}

func TestAllShardsCloneWithTargetData(t *testing.T) {
	_, _, err := startAll()
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()

	rowCount := 500
	// Insert some data to source
	err = insertBunchaData(context.Background(), source, rowCount)
	assert.NoError(t, err)

	// Insert even more data to target; rows which don't exist in any source shard should get deleted.
	err = insertBunchaData(context.Background(), target, rowCount+100)
	assert.NoError(t, err)

	// Clone left shard, -80
	source.Database = "customer/-80@replica"
	readerConfig := ReaderConfig{
		SourceTargetConfig: SourceTargetConfig{
			Source: source,
			Target: target,
		},
		ThroughputLoggingFrequency: 100 * time.Millisecond,
		ChunkSize:                  5, // Smaller chunk size to make sure we're exercising chunking
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
	}
	clone := &Clone{
		WriterConfig: WriterConfig{
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

	// Clone right shard, 80-
	source.Database = "customer/80-@replica"
	readerConfig = ReaderConfig{
		SourceTargetConfig: SourceTargetConfig{
			Source: source,
			Target: target,
		},
		ThroughputLoggingFrequency: 100 * time.Millisecond,
		ChunkSize:                  5, // Smaller chunk size to make sure we're exercising chunking
		Config: Config{
			Tables: map[string]TableConfig{
				"customers": {
					// equivalent to 80-
					TargetWhere:    "(vitess_hash(id) >> 56) >= 128 and (vitess_hash(id) >> 56) < 256",
					WriteBatchSize: 5, // Smaller batch size to make sure we're exercising batching
				},
				"transactions": {
					// equivalent to 80-
					TargetWhere:    "(vitess_hash(customer_id) >> 56) >= 128 and (vitess_hash(customer_id) >> 56) < 256",
					WriteBatchSize: 5, // Smaller batch size to make sure we're exercising batching
					KeyColumns:     []string{"customer_id", "id"},
				},
			},
		},
	}
	clone = &Clone{
		WriterConfig: WriterConfig{
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
	source.Database = "customer@master"
	sourceRowCount, err := countRows(source, "customers")
	assert.NoError(t, err)
	targetRowCount, err := countRows(target, "customers")
	assert.NoError(t, err)
	assert.Equal(t, sourceRowCount, targetRowCount)

	// Do a full checksum
	checksum := &Checksum{
		ReaderConfig: readerConfig,
	}
	err = kong.ApplyDefaults(checksum)
	// Turn on CRC32 checksum, it works on shard targeted clones from Vitess!
	checksum.UseCRC32Checksum = true
	checksum.IgnoreReplicationLag = true
	assert.NoError(t, err)
	diffs, err := checksum.run(context.Background())
	assert.NoError(t, err)
	err = reportDiffs(diffs)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(diffs))
}

func TestUnshardedCloneEmptySourceTables(t *testing.T) {
	_, _, err := startAll()
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()

	// Make sure source tables are empty
	err = clearTables(context.Background(), source)
	assert.NoError(t, err)
	// Insert data only to target; the expectation is for target to be empty when Cloner is done
	err = insertBunchaData(context.Background(), target, 1000)
	assert.NoError(t, err)

	source.Database = "@replica"
	clone := &Clone{
		WriterConfig: WriterConfig{
			ReaderConfig: ReaderConfig{
				SourceTargetConfig: SourceTargetConfig{
					Source: source,
					Target: target,
				},
				ThroughputLoggingFrequency: 100 * time.Millisecond,
				ChunkSize:                  5, // Smaller chunk size to make sure we're exercising chunking
				WriteBatchSize:             5, // Smaller batch size to make sure we're exercising batching
			},
			WriteBatchStatementSize: 3, // Smaller batch size to make sure we're exercising batching
		},
	}
	err = kong.ApplyDefaults(clone)
	assert.NoError(t, err)
	err = clone.Run()
	assert.NoError(t, err)

	targetRowCount, err := countRows(target, "customers")
	assert.NoError(t, err)
	assert.Equal(t, 0, targetRowCount)
	targetRowCount, err = countRows(target, "transactions")
	assert.NoError(t, err)
	assert.Equal(t, 0, targetRowCount)
}
