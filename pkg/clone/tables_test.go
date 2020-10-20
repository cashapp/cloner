package clone

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestLoadTables(t *testing.T) {
	ctx := context.Background()

	db, err := vitessContainer.Config().DB()
	assert.NoError(t, err)
	conn, err := db.Conn(ctx)
	assert.NoError(t, err)
	tables, err := LoadTables(ctx, Vitess, conn, "customer")
	assert.NoError(t, err)
	assert.Equal(t, []*Table{
		{
			Name:                "customers",
			IDColumn:            "id",
			IDColumnIndex:       0,
			ShardingColumn:      "id",
			ShardingColumnIndex: 0,
			Columns:             []string{"id", "name"},
		},
	}, tables)
}

func dropTables(config DBConfig) error {
	db, err := config.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = db.Exec("DROP TABLE customers")
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func TestCopySchema(t *testing.T) {
	source := vitessContainer.Config()
	source.Database = "customer/-80"
	target := mysqlContainer.Config()

	err := dropTables(target)
	assert.NoError(t, err)

	ctx := context.Background()

	sourceDB, err := source.DB()
	assert.NoError(t, err)
	sourceConn, err := sourceDB.Conn(ctx)
	assert.NoError(t, err)
	targetDB, err := target.DB()
	assert.NoError(t, err)
	targetConn, err := targetDB.Conn(ctx)
	assert.NoError(t, err)

	tables, err := LoadTables(ctx, Vitess, sourceConn, "customer")
	err = CopySchema(ctx, tables, sourceConn, targetConn)
	assert.NoError(t, err)
}
