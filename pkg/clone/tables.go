package clone

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"strings"

	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/pkg/errors"
	"vitess.io/vitess/go/vt/proto/query"
)

type Table struct {
	Name string

	// IDColumn is the name of the ID column
	IDColumn string
	// IDColumnIndex is the index of the ID column in the Columns field
	IDColumnIndex int

	// KeyColumns is the columns the table is chunked by, by default the primary key columns
	KeyColumns []string
	// KeyColumnList is KeyColumns quoted and comma separated
	KeyColumnList string
	// KeyColumnIndexes is KeyColumns quoted and comma separated
	KeyColumnIndexes []int

	Config TableConfig

	Columns       []string
	ColumnsQuoted []string
	CRC32Columns  []string
	ColumnList    string
	EstimatedRows int64

	// MysqlTable is the go-mysql schema, we should probably start using this one as much as possible
	MysqlTable *mysqlschema.Table
}

func (t *Table) PkOfRow(row []interface{}) int64 {
	pks, err := t.MysqlTable.GetPKValues(row)
	if err != nil {
		panic(err)
	}
	pk := pks[0]
	i, err := coerceInt64(pk)
	if err != nil {
		// we can panic because we should have validated the table already at load time?
		panic(err.Error())
	}
	return i
}

func (t *Table) Validate() error {
	// Validate which PK columns we currently support
	if len(t.MysqlTable.PKColumns) != 1 {
		return errors.Errorf("currently only support a single PK column")
	}
	pkColumn := t.MysqlTable.GetPKColumn(0)
	if pkColumn.Type != mysqlschema.TYPE_NUMBER {
		return errors.Errorf("currently only support integer PK column")
	}
	return nil
}

func (t *Table) ToRow(raw []interface{}) *Row {
	return &Row{
		Table: t,
		Data:  raw,
	}
}

func (t *Table) KeysOfRow(row []interface{}) []interface{} {
	keys := make([]interface{}, len(t.KeyColumnIndexes))
	for i, index := range t.KeyColumnIndexes {
		keys[i] = row[index]
	}
	return keys
}

func LoadTables(ctx context.Context, config ReaderConfig) ([]*Table, error) {
	var err error

	// If the source has keyspace use that, otherwise use the target schema
	var dbConfig DBConfig
	sourceSchema, err := config.Source.Schema()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if sourceSchema != "" {
		// TODO if using the source filter the tables with the target schema unless we're doing a consistent clone
		dbConfig = config.Source
	} else {
		dbConfig = config.Target
	}

	db, err := dbConfig.ReaderDB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer db.Close()

	retry := RetryOptions{
		MaxRetries: config.ReadRetries,
		Timeout:    config.ReadTimeout,
	}
	var tables []*Table
	err = Retry(ctx, retry, func(ctx context.Context) error {
		tables, err = loadTables(ctx, config, dbConfig, db)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(tables) == 0 {
			return errors.Errorf("no tables found")
		}
		return err
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Shuffle the tables so they are processed in random order (which spreads out load)
	rand.Shuffle(len(tables), func(i, j int) { tables[i], tables[j] = tables[j], tables[i] })
	return tables, nil
}

func loadTables(ctx context.Context, config ReaderConfig, dbConfig DBConfig, db *sql.DB) ([]*Table, error) {
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
		table, err := loadTable(ctx, config, dbConfig.Type, db, schema, tableName, config.Config.Tables[tableName])
		if err != nil {
			return nil, err
		}
		err = table.Validate()
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

func loadTable(ctx context.Context, config ReaderConfig, databaseType DataSourceType, conn *sql.DB, schema, tableName string, tableConfig TableConfig) (*Table, error) {
	var err error
	var rows *sql.Rows
	var internalTableSchema string
	if databaseType == MySQL {
		rows, err = conn.QueryContext(ctx,
			"select table_schema from information_schema.tables where table_schema = ? and table_name = ?",
			schema, tableName)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		defer rows.Close()
	} else if databaseType == Vitess {
		rows, err = conn.QueryContext(ctx,
			"select table_schema from information_schema.columns where table_name = ? and table_schema like ?",
			tableName, fmt.Sprintf("vt_%s%%", schema))
		if err != nil {
			return nil, errors.WithStack(err)
		}
		defer rows.Close()
	} else {
		return nil, errors.Errorf("Not supported: %v", databaseType)
	}
	if !rows.Next() {
		return nil, errors.Errorf("table %v not found in information_schema", tableName)
	}
	err = rows.Scan(&internalTableSchema)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mysqlTable, err := mysqlschema.NewTableFromSqlDB(conn, internalTableSchema, tableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}

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
		return nil, errors.WithStack(err)
	}

	estimatedRows, err := estimatedRows(ctx, databaseType, conn, schema, tableName)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	if tableConfig.ChunkSize == 0 {
		tableConfig.ChunkSize = config.ChunkSize
	}
	if tableConfig.WriteBatchSize == 0 {
		tableConfig.WriteBatchSize = config.WriteBatchSize
	}

	table := &Table{
		Name:          tableName,
		Columns:       columnNames,
		ColumnsQuoted: columnNamesQuoted,
		CRC32Columns:  columnNamesCRC32,
		ColumnList:    strings.Join(columnNamesQuoted, ","),
		EstimatedRows: estimatedRows,
		Config:        tableConfig,
		MysqlTable:    mysqlTable,
	}

	if len(mysqlTable.PKColumns) >= 1 {
		// TODO we should be really close to supporting multiple pk columns now
		pkColumn := mysqlTable.GetPKColumn(0)
		table.IDColumn = pkColumn.Name
		for i, column := range columnNames {
			if column == table.IDColumn {
				table.IDColumnIndex = i
				break
			}
		}
		table.KeyColumns = table.Config.KeyColumns
		if len(table.KeyColumns) == 0 {
			table.KeyColumns = []string{table.IDColumn}
		}
		for _, keyColumn := range table.KeyColumns {
			for columnIndex, column := range table.Columns {
				if column == keyColumn {
					table.KeyColumnIndexes = append(table.KeyColumnIndexes, columnIndex)
				}
			}
		}
		if len(table.KeyColumns) != len(table.KeyColumnIndexes) {
			return nil, errors.Errorf("did not find all the key columns %v in column list %v",
				table.KeyColumns, table.Columns)
		}
	}

	var keyColumnList strings.Builder
	for i, column := range table.KeyColumns {
		keyColumnList.WriteString("`")
		keyColumnList.WriteString(column)
		keyColumnList.WriteString("`")
		if i < len(table.KeyColumns)-1 {
			keyColumnList.WriteString(", ")
		}
	}
	table.KeyColumnList = keyColumnList.String()

	return table, nil
}

func estimatedRows(ctx context.Context, databaseType DataSourceType, conn DBReader, schema string, tableName string) (int64, error) {
	var err error
	var rows *sql.Rows
	if databaseType == MySQL {
		rows, err = conn.QueryContext(ctx,
			"select round(data_length/avg_row_length) from information_schema.tables where table_schema = ? and table_name = ?",
			schema, tableName)
		if err != nil {
			return 0, errors.WithStack(err)
		}
		defer rows.Close()
	} else if databaseType == Vitess {
		rows, err = conn.QueryContext(ctx,
			"select round(data_length/avg_row_length) from information_schema.tables where table_name = ? and table_schema like ?",
			tableName, fmt.Sprintf("vt_%s%%", schema))
		if err != nil {
			return 0, errors.WithStack(err)
		}
		defer rows.Close()
	} else {
		return 0, errors.Errorf("not supported: %v", databaseType)
	}
	if !rows.Next() {
		return 0, errors.Errorf("could not estimate number of rows of table %v.%v", schema, tableName)
	}

	var estimatedRows *int64
	err = rows.Scan(&estimatedRows)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	if estimatedRows == nil {
		// Empty table most likely
		return 0, nil
	}
	return *estimatedRows, nil
}
