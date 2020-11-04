package clone

import (
	"context"
	"testing"
	"time"

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
	tables, err := LoadTables(ctx, Vitess, conn, "customer", true, nil, 1*time.Second)
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
