package clone

import (
	"context"
	"testing"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestLoadTables(t *testing.T) {
	err := startVitess()
	assert.NoError(t, err)

	ctx := context.Background()

	db, err := vitessContainer.Config().DB()
	assert.NoError(t, err)
	conn, err := db.Conn(ctx)
	assert.NoError(t, err)
	tables, err := LoadTables(ctx, Vitess, conn, "customer", true, nil)
	assert.NoError(t, err)
	assert.Equal(t, []*Table{
		{
			Name:                "customers",
			IDColumn:            "id",
			IDColumnIndex:       0,
			ShardingColumn:      "id",
			ShardingColumnIndex: 0,
			Columns:             []string{"id", "name"},
			ColumnsQuoted:       []string{"`id`", "`name`"},
			ColumnList:          "`id`,`name`",
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
