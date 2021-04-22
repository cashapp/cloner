package clone

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"vitess.io/vitess/go/vt/proto/query"
)

type Table struct {
	Name string

	// ShardingColumn is the name of the ID column
	IDColumn string
	// ShardingColumnIndex is the index of the ID column in the Columns field
	IDColumnIndex int

	Config TableConfig

	Columns       []string
	ColumnsQuoted []string
	CRC32Columns  []string
	ColumnList    string
}

func LoadTables(ctx context.Context, config ReaderConfig) ([]*Table, error) {
	var err error

	// If the source has keyspace use that, otherwise use the target schema
	dbConfig := config.Target
	sourceSchema, err := config.Source.Schema()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if sourceSchema != "" {
		// TODO if using the source filter the tables with the target schema unless we're doing a consistent clone
		dbConfig = config.Source
	}

	db, err := dbConfig.ReaderDB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer db.Close()

	var tables []*Table
	b := backoff.WithContext(backoff.WithMaxRetries(InfiniteExponentialBackOff(), config.ReadRetries), ctx)
	err = backoff.Retry(func() error {
		ctx, cancel := context.WithTimeout(ctx, config.ReadTimeout)
		defer cancel()

		tables, err = loadTables(ctx, config, dbConfig, db)
		if len(tables) == 0 {
			return errors.Errorf("no tables found")
		}
		return err
	}, b)
	// Shuffle the tables so they are processed in random order (which spreads out load)
	rand.Shuffle(len(tables), func(i, j int) { tables[i], tables[j] = tables[j], tables[i] })
	return tables, err
}

func loadTables(ctx context.Context, config ReaderConfig, dbConfig DBConfig, db DBReader) ([]*Table, error) {
	schema, err := dbConfig.Schema()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var tableNames []string

	if len(config.Config.Tables) == 0 {
		var err error
		var rows *sql.Rows
		if dbConfig.Type == MySQL {
			rows, err = db.QueryContext(ctx,
				"select table_name from information_schema.tables where table_schema = ?", schema)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			defer rows.Close()
		} else if dbConfig.Type == Vitess {
			rows, err = db.QueryContext(ctx,
				"select table_name from information_schema.tables where table_schema like ?",
				fmt.Sprintf("vt_%s%%", schema))
			if err != nil {
				return nil, errors.WithStack(err)
			}
			defer rows.Close()
		} else {
			return nil, errors.Errorf("Not supported: %v", dbConfig.Type)
		}
		for rows.Next() {
			var tableName string
			err := rows.Scan(&tableName)
			if err != nil {
				return nil, errors.WithStack(err)
			}
			// There are duplicates with vttestserver because multiples shards run in the same mysqld
			if !contains(tableNames, tableName) {
				tableNames = append(tableNames, tableName)
			}
		}
		err = rows.Close()
		if err != nil {
			return nil, err
		}
	} else {
		tableNames = make([]string, 0, len(config.Config.Tables))
		for t := range config.Config.Tables {
			tableNames = append(tableNames, t)
		}
	}
	tables := make([]*Table, 0, len(tableNames))
	for _, tableName := range tableNames {
		// Ignore pt-osc tables
		// TODO implement table include/exclude regexps instead
		if strings.HasPrefix(tableName, "ptosc_") {
			continue
		}
		if strings.HasSuffix(tableName, "_seq") {
			continue
		}
		if strings.HasSuffix(tableName, "_lookup") {
			continue
		}
		if tableName == "schema_version" {
			continue
		}
		table, err := loadTable(ctx, config, dbConfig.Type, db, schema, tableName, config.Config.Tables[tableName])
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
	}
	return tables, nil
}

func isSharded(spec *query.Target) bool {
	return spec.Shard != "0" && spec.Shard != "-"
}

func contains(strings []string, str string) bool {
	for _, s := range strings {
		if s == str {
			return true
		}
	}
	return false
}

func loadTable(ctx context.Context, config ReaderConfig, databaseType DataSourceType, conn DBReader, schema, tableName string, tableConfig TableConfig) (*Table, error) {
	var err error
	var rows *sql.Rows
	if databaseType == MySQL {
		rows, err = conn.QueryContext(ctx,
			"select column_name from information_schema.columns where table_schema = ? and table_name = ?",
			schema, tableName)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		defer rows.Close()
	} else if databaseType == Vitess {
		rows, err = conn.QueryContext(ctx,
			"select column_name from information_schema.columns where table_name = ? and table_schema like ?",
			tableName, fmt.Sprintf("vt_%s%%", schema))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		defer rows.Close()
	} else {
		return nil, errors.Errorf("Not supported: %v", databaseType)
	}
	var columnNames []string
	var columnNamesQuoted []string
	var columnNamesCRC32 []string
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// There are duplicates with vttestserver because multiples shards run in the same mysqld
		if contains(columnNames, columnName) {
			continue
		}
		if contains(tableConfig.IgnoreColumns, columnName) {
			continue
		}
		columnNames = append(columnNames, columnName)
		columnNamesQuoted = append(columnNamesQuoted, fmt.Sprintf("`%s`", columnName))
		columnNamesCRC32 = append(columnNamesCRC32, fmt.Sprintf("crc32(ifnull(`%s`, 0))", columnName))
	}
	// Close explicitly to check for close errors
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	// Yep, hardcoded, maybe we should fix that at some point...
	idColumn := "id"
	idColumnIndex := -1
	for i, column := range columnNames {
		if column == idColumn {
			idColumnIndex = i
		}
	}
	return &Table{
		Name:          tableName,
		IDColumn:      idColumn,
		IDColumnIndex: idColumnIndex,
		Columns:       columnNames,
		ColumnsQuoted: columnNamesQuoted,
		CRC32Columns:  columnNamesCRC32,
		ColumnList:    strings.Join(columnNamesQuoted, ","),
		Config:        tableConfig,
	}, nil
}
