package clone

import (
	"context"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
)

func TestChecksum(t *testing.T) {
	_, _, err := startAll()
	assert.NoError(t, err)

	rowCount := 1000
	err = insertBunchaData(context.Background(), vitessContainer.Config(), rowCount)
	assert.NoError(t, err)

	err = deleteAllData(tidbContainer.Config())
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()
	source.Database = "customer/-80@replica"

	// Check how many rows end up in the -80 shard
	customerCount, err := countRows(source, "customers")
	assert.NoError(t, err)
	transactionCount, err := countRows(source, "transactions")
	assert.NoError(t, err)

	checksum := &Checksum{
		IgnoreReplicationLag: true,
		ReaderConfig: ReaderConfig{
			SourceTargetConfig: SourceTargetConfig{
				Source: source,
				Target: target,
			},
			ChunkSize: 5, // Smaller chunk size to make sure we're exercising chunking
			Config: Config{
				Tables: map[string]TableConfig{
					"customers":    {},
					"transactions": {KeyColumns: []string{"customer_id", "id"}},
				},
			},
		},
	}
	err = kong.ApplyDefaults(checksum)
	assert.NoError(t, err)
	diffs, err := checksum.run(context.Background())
	assert.NoError(t, err)
	checksum.reportDiffs(diffs)
	assert.Equal(t, customerCount+transactionCount, len(diffs))
}

func TestChecksumWithRepair(t *testing.T) {
	source, err := startMysql()
	assert.NoError(t, err)
	defer source.Close()
	err = insertBunchaData(context.Background(), source.Config(), 1000)
	assert.NoError(t, err)

	target, err := startMysql()
	assert.NoError(t, err)
	defer target.Close()
	err = insertBunchaData(context.Background(), target.Config(), 10)
	assert.NoError(t, err)

	checksum := &Checksum{
		IgnoreReplicationLag: true,
		ReaderConfig: ReaderConfig{
			SourceTargetConfig: SourceTargetConfig{
				Source: source.Config(),
				Target: target.Config(),
			},
			ChunkSize: 5, // Smaller chunk size to make sure we're exercising chunking
			Config: Config{
				Tables: map[string]TableConfig{
					"customers":    {},
					"transactions": {KeyColumns: []string{"customer_id", "id"}},
				},
			},
		},
	}
	err = kong.ApplyDefaults(checksum)
	assert.NoError(t, err)
	diffs, err := checksum.run(context.Background())
	assert.NoError(t, err)
	checksum.reportDiffs(diffs)
	assert.Equal(t, 2000, len(diffs))

	checksum.RepairAttempts = 1
	diffs, err = checksum.repairDiffs(context.Background(), diffs)
	assert.NoError(t, err)
	assert.Equal(t, 0, len(diffs))
}
