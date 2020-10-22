package clone

import (
	"testing"

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
	rowCount := 1000
	err := insertBunchaData(vitessContainer.Config(), "Name", rowCount)
	assert.NoError(t, err)

	err = deleteAllData(tidbContainer.Config())
	assert.NoError(t, err)

	source := vitessContainer.Config()
	target := tidbContainer.Config()
	source.Database = "customer/-80@replica"

	// Check how many rows end up in the -80 shard
	shardRowCount, err := countRows(source)

	checksum := &Checksum{
		HighFidelity:   false,
		QueueSize:      1000,
		ChunkSize:      5,
		WriteBatchSize: 5,
		ChunkerCount:   1,
		ReaderCount:    1,
		WriterCount:    1,
	}
	diffs, err := checksum.run(Globals{
		Source: source,
		Target: target,
	})
	assert.NoError(t, err)
	assert.Equal(t, shardRowCount, len(diffs))
}
