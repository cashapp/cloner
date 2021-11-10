package clone

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestLoadTablesShardedVitess(t *testing.T) {
	err := startVitess()
	assert.NoError(t, err)

	ctx := context.Background()

	source := vitessContainer.Config()
	source.Database = "customer/-80@replica"
	config := ReaderConfig{ReadTimeout: time.Second,
		Config: Config{
			Tables: map[string]TableConfig{
				"customers": {},
			},
		},
		SourceTargetConfig: SourceTargetConfig{Source: source}}

	tables, err := LoadTables(ctx, config)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(tables))
	// not testing the content of this one
	tables[0].MysqlTable = nil
	assert.Equal(t, []*Table{
		{
			Name:             "customers",
			KeyColumns:       []string{"id"},
			KeyColumnList:    "`id`",
			KeyColumnIndexes: []int{0},
			Columns:          []string{"id", "name"},
			ColumnsQuoted:    []string{"`id`", "`name`"},
			CRC32Columns: []string{
				"crc32(ifnull(`id`, 0))",
				"crc32(ifnull(`name`, 0))",
			},
			ColumnList: "`id`,`name`",
		},
	}, tables)
}

func TestLoadTablesUnshardedVitess(t *testing.T) {
	t.Skip("unsharded vitess as a source is not currently supported")

	err := startVitess()
	assert.NoError(t, err)

	ctx := context.Background()

	config := ReaderConfig{ReadTimeout: time.Second,
		Config: Config{
			Tables: map[string]TableConfig{
				"customers": {},
			},
		},
		SourceTargetConfig: SourceTargetConfig{Source: vitessContainer.Config()}}

	tables, err := LoadTables(ctx, config)
	assert.NoError(t, err)
	assert.Equal(t, []*Table{
		{
			Name:             "customers",
			KeyColumns:       []string{"id"},
			KeyColumnList:    "`id`",
			KeyColumnIndexes: []int{0},
			Columns:          []string{"id", "name"},
			ColumnsQuoted:    []string{"`id`", "`name`"},
			CRC32Columns: []string{
				"crc32(ifnull(`id`, 0))",
				"crc32(ifnull(`name`, 0))",
			},
			ColumnList: "`id`,`name`",
		},
	}, tables)
}

func TestLoadTablesTiDB(t *testing.T) {
	t.Skip("unsharded vitess as a source is not currently supported")

	err := startTidb()
	assert.NoError(t, err)

	ctx := context.Background()

	config := ReaderConfig{ReadTimeout: time.Second,
		Config: Config{
			Tables: map[string]TableConfig{
				"customers": {},
			},
		},
		SourceTargetConfig: SourceTargetConfig{Source: tidbContainer.Config()}}
	tables, err := LoadTables(ctx, config)
	assert.NoError(t, err)
	assert.Equal(t, []*Table{
		{
			Name:             "customers",
			KeyColumns:       []string{"id"},
			KeyColumnList:    "`id`",
			KeyColumnIndexes: []int{0},
			Columns:          []string{"id", "name"},
			ColumnsQuoted:    []string{"`id`", "`name`"},
			CRC32Columns: []string{
				"crc32(ifnull(`id`, 0))",
				"crc32(ifnull(`name`, 0))",
			},
			ColumnList: "`id`,`name`",
		},
	}, tables)
}
