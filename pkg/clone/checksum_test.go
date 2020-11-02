package clone

import (
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func deleteAllData(config DBConfig) error {
	db, err := config.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = db.Exec("DELETE FROM customers")
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func TestChecksum(t *testing.T) {
	// Restart vitess for this test so other tests don't mess with our auto increment IDs
	vitessContainer.Close()
	vitessContainer, err := startVitess()
	assert.NoError(t, err)
	tidbContainer.Close()
	tidbContainer, err := startTidb()
	assert.NoError(t, err)

	rowCount := 1000
	err = insertBunchaData(vitessContainer.Config(), "Name", rowCount)
	assert.NoError(t, err)

	err = deleteAllData(tidbContainer.Config())
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()
	source.Database = "customer/-80@replica"

	// Check how many rows end up in the -80 shard
	shardRowCount, err := countRows(source, "customers")
	assert.NoError(t, err)

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
	assert.Equal(t, shardRowCount, len(diffs))
}
