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
			Name:          "customers",
			IDColumn:      "id",
			IDColumnIndex: 0,
			Columns:       []string{"id", "name"},
			ColumnsQuoted: []string{"`id`", "`name`"},
			CRC32Columns: []string{
				"crc32(ifnull(`id`, 0))",
				"crc32(ifnull(`name`, 0))",
			},
			ColumnList: "`id`,`name`",
		},
	}, tables)
}

func TestLoadTablesTiDB(t *testing.T) {
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
			Name:          "customers",
			IDColumn:      "id",
			IDColumnIndex: 0,
			Columns:       []string{"id", "name"},
			ColumnsQuoted: []string{"`id`", "`name`"},
			CRC32Columns: []string{
				"crc32(ifnull(`id`, 0))",
				"crc32(ifnull(`name`, 0))",
			},
			ColumnList: "`id`,`name`",
		},
	}, tables)
}
