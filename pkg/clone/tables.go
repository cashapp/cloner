package clone

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/pkg/errors"
)

type Table struct {
	Name string

	// ShardingColumn is the name of the ID column
	IDColumn string
	// ShardingColumnIndex is the index of the ID column in the Columns field
	IDColumnIndex int

	// ShardingColumn is the column we shard by
	ShardingColumn string
	// ShardingColumnIndex is the index of the sharding column in the Columns field
	ShardingColumnIndex int

	Columns []string
}

func LoadTables(ctx context.Context, databaseType DataSourceType, conn *sql.Conn, schema string) ([]*Table, error) {
	var err error
	var rows *sql.Rows
	if databaseType == MySQL {
		rows, err = conn.QueryContext(ctx,
			"select table_name from information_schema.tables where table_schema = ?", schema)
	} else if databaseType == Vitess {
		rows, err = conn.QueryContext(ctx,
			"select table_name from information_schema.tables where table_schema like ?",
			fmt.Sprintf("vt_%s%%", schema))
	} else {
		return nil, errors.Errorf("Not supported: %v", databaseType)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var tableNames []string
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
	tables := make([]*Table, len(tableNames))
	for i, tableName := range tableNames {
		table, err := loadTable(ctx, databaseType, conn, schema, tableName)
		if err != nil {
			return nil, err
		}
		tables[i] = table
	}
	return tables, nil
}

func contains(strings []string, str string) bool {
	for _, s := range strings {
		if s == str {
			return true
		}
	}
	return false
}

func loadTable(ctx context.Context, databaseType DataSourceType, conn *sql.Conn, schema, tableName string) (*Table, error) {
	var err error
	var rows *sql.Rows
	if databaseType == MySQL {
		rows, err = conn.QueryContext(ctx,
			"select column_name from information_schema.columns where table_schema = ? and table_name = ?",
			schema, tableName)
	} else if databaseType == Vitess {
		rows, err = conn.QueryContext(ctx,
			"select column_name from information_schema.columns where table_name = ? and table_schema like ?",
			tableName, fmt.Sprintf("vt_%s%%", schema))
	} else {
		return nil, errors.Errorf("Not supported: %v", databaseType)
	}
	defer rows.Close()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	var columnNames []string
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// There are duplicates with vttestserver because multiples shards run in the same mysqld
		if !contains(columnNames, columnName) {
			columnNames = append(columnNames, columnName)
		}
	}
	// Close explicitly to check for close errors
	err = rows.Close()
	if err != nil {
		return nil, err
	}
	// Yep, hardcoded, maybe we should fix that at some point...
	idColumn := "id"
	var shardingColumn string
	// Yes hardcoded for now, ideally we should look this up in the vschema
	if tableName == "customers" {
		shardingColumn = "id"
	} else {
		shardingColumn = "customer_id"
	}
	idColumnIndex := -1
	shardingColumnIndex := -1
	for i, column := range columnNames {
		if column == idColumn {
			idColumnIndex = i
		}
		if column == shardingColumn {
			shardingColumnIndex = i
		}
	}
	return &Table{
		Name:                tableName,
		IDColumn:            idColumn,
		IDColumnIndex:       idColumnIndex,
		ShardingColumn:      shardingColumn,
		ShardingColumnIndex: shardingColumnIndex,
		Columns:             columnNames,
	}, nil
}
