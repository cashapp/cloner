package clone

import (
	"context"
	"testing"

	"github.com/alecthomas/kong"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func deleteAllData(config DBConfig) error {
	db, err := config.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer db.Close()

	_, err = db.Exec("DELETE FROM customers")
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = db.Exec("DELETE FROM transactions")
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

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
