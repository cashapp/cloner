package clone

import (
	"context"
	"fmt"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"vitess.io/vitess/go/vt/key"
)

func insertBunchaData(config DBConfig, rowCount int) error {
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
	`, fmt.Sprintf("Name %d", i))
	}
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func countRows(target DBConfig) (int, error) {
	db, err := target.DB()
	if err != nil {
		return 0, errors.WithStack(err)
	}
	ctx := context.Background()
	conn, err := db.Conn(ctx)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	row := conn.QueryRowContext(ctx, "SELECT COUNT(*) FROM customers")
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

func TestClone(t *testing.T) {
	source := vitessContainer.Config()
	target := mysqlContainer.Config()

	rowCount := 1000
	err := insertBunchaData(source, rowCount)
	assert.NoError(t, err)

	// Insert some stuff in the target too so that there's a real diff
	err = insertBunchaData(target, 50)
	// The clone should not touch the rows in the right 80- shard
	rightRowCountBefore, err := countRowsShardFilter(target, "80-")
	assert.NoError(t, err)

	clone := &Clone{
		HighFidelity:   false,
		QueueSize:      1000,
		ChunkSize:      5,
		WriteBatchSize: 5,
		ChunkerCount:   1,
		ReaderCount:    1,
		WriterCount:    1,
	}
	source.Database = "customer/-80@replica"
	assert.NoError(t, err)
	err = clone.Run(Globals{
		Source: source,
		Target: target,
	})
	assert.NoError(t, err)

	// Sanity check the number of rows
	sourceRowCount, err := countRows(source)
	targetRowCount, err := countRowsShardFilter(target, "-80")
	assert.NoError(t, err)
	assert.Equal(t, sourceRowCount, targetRowCount)

	// Check we didn't delete the rows in the right shard in the target
	rightRowCountAfter, err := countRowsShardFilter(target, "80-")
	assert.Equal(t, rightRowCountBefore, rightRowCountAfter)

	// Do a full checksum
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
	assert.Equal(t, 0, len(diffs))
}
