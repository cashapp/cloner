package clone

import (
	"context"
	"testing"
	"time"

	"github.com/go-mysql-org/go-mysql/schema"

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
	tables[0].EstimatedRows = 0
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
			IgnoredColumnsBitmap: []bool{
				false,
				false,
			},
			ColumnList: "`id`,`name`",
		},
	}, tables)
}

func TestLoadTablesUnshardedVitess(t *testing.T) {
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

	err = deleteAllData(vitessContainer.Config())
	assert.NoError(t, err)

	tables, err := LoadTables(ctx, config)
	assert.NoError(t, err)
	tables[0].EstimatedRows = 0
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
			IgnoredColumnsBitmap: []bool{
				false,
				false,
			},
			MysqlTable: &schema.Table{
				Schema: "customer",
				Name:   "customers",
				Columns: []schema.TableColumn{
					{
						Name:    "id",
						Type:    1,
						RawType: "bigint(20)",
						IsAuto:  true,
					},
					{
						Name:      "name",
						Type:      5,
						Collation: "utf8mb4_general_ci",
						RawType:   "varchar(255)",
						MaxSize:   255,
					},
				},
				Indexes: []*schema.Index{
					{
						Name: "PRIMARY",
						Columns: []string{
							"id",
						},
						Cardinality: []uint64{
							1,
						},
					},
				},
				PKColumns: []int{0},
			},
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
	tables[0].EstimatedRows = 0
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
			IgnoredColumnsBitmap: []bool{
				false,
				false,
			},
			MysqlTable: &schema.Table{
				Schema: "mydatabase",
				Name:   "customers",
				Columns: []schema.TableColumn{
					{
						Name:    "id",
						Type:    1,
						RawType: "bigint(20)",
						IsAuto:  true,
					},
					{
						Name:      "name",
						Type:      5,
						Collation: "utf8mb4_bin",
						RawType:   "varchar(255)",
						MaxSize:   255,
					},
				},
				Indexes: []*schema.Index{
					{
						Name: "PRIMARY",
						Columns: []string{
							"id",
						},
						Cardinality: []uint64{
							1,
						},
					},
				},
				PKColumns: []int{0},
			},
		},
	}, tables)
}
