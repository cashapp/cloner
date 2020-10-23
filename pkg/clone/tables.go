package clone

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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

	Columns       []string
	ColumnsQuoted []string
	ColumnList    string
}

func LoadTables(ctx context.Context, databaseType DataSourceType, conn *sql.Conn, schema string, includeTables []string) ([]*Table, error) {
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
	tables := make([]*Table, 0, len(tableNames))
	for _, tableName := range tableNames {
		// Ignore pt-osc tables
		if strings.HasPrefix(tableName, "ptosc_") {
			continue
		}
		if tableName == "schema_version" {
			continue
		}
		if len(includeTables) > 0 && !contains(includeTables, tableName) {
			continue
		}
		table, err := loadTable(ctx, databaseType, conn, schema, tableName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, table)
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
	var columnNamesQuoted []string
	for rows.Next() {
		var columnName string
		err := rows.Scan(&columnName)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// There are duplicates with vttestserver because multiples shards run in the same mysqld
		if !contains(columnNames, columnName) {
			columnNames = append(columnNames, columnName)
			columnNamesQuoted = append(columnNamesQuoted, fmt.Sprintf("`%s`", columnName))
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
	switch tableName {
	case "customers":
		shardingColumn = "id"
	case "abuse_reports":
		shardingColumn = "reporter_customer_id"
	case "asset_verification_attempts":
		shardingColumn = "target_customer_id"
	case "archiver_messages":
		shardingColumn = "entity_group_root_id"
	case "campaign_donations":
		shardingColumn = "sender_id"
	case "campaign_enrollments":
		shardingColumn = "candidate_customer_id"
	case "evently_sharded_producer_actions":
		shardingColumn = "local_shard_key"
	case "referrals":
		shardingColumn = "referrer_customer_id"
	case "reward_payments":
		shardingColumn = "recipient_id"
	case "sharded_spooled_immediate_jobs":
		shardingColumn = "cid"
	case "sharded_spooled_future_jobs":
		shardingColumn = "cid"
	case "known_aliases":
		shardingColumn = "source_customer_id"
	default:
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
	if shardingColumnIndex == -1 {
		return nil, errors.Errorf("sharding column not found for %v", tableName)
	}
	return &Table{
		Name:                tableName,
		IDColumn:            idColumn,
		IDColumnIndex:       idColumnIndex,
		ShardingColumn:      shardingColumn,
		ShardingColumnIndex: shardingColumnIndex,
		Columns:             columnNames,
		ColumnsQuoted:       columnNamesQuoted,
		ColumnList:          strings.Join(columnNamesQuoted, ","),
	}, nil
}

func CopySchema(ctx context.Context, tables []*Table, source *sql.Conn, target *sql.DB) error {
	for _, table := range tables {
		row := source.QueryRowContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %s", table.Name))
		var name string
		var createTable string
		err := row.Scan(&name, &createTable)
		if err != nil {
			return errors.WithStack(err)
		}
		createTable = strings.Replace(createTable, "CREATE TABLE", "CREATE TABLE IF NOT EXISTS", 1)
		// TODO figure out if this is really right...
		createTable = strings.ReplaceAll(createTable,
			"timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'",
			"timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP")
		_, err = target.ExecContext(ctx, createTable)
		if err != nil {
			return errors.Wrapf(err, "could not create table:\n%s", createTable)
		}
	}
	return nil
}
