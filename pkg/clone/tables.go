package clone

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"regexp"
	"strings"

	"github.com/sirupsen/logrus"

	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/pkg/errors"
	"vitess.io/vitess/go/vt/proto/query"
)

type Table struct {
	Name string

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
	// ColumnList is a comma separated list of quoted strings
	ColumnList           string
	EstimatedRows        int64
	IgnoredColumnsBitmap []bool

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

	if len(config.Tables) > 0 {
		var filteredTables []*Table
		for _, table := range tables {
			for _, checksumTable := range config.Tables {
				if checksumTable == table.Name {
					filteredTables = append(filteredTables, table)
				}
			}
		}
		tables = filteredTables
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
			return nil, errors.WithStack(err)
		}
	} else {
		tableNames = make([]string, 0, len(config.Config.Tables))
		for t := range config.Config.Tables {
			tableNames = append(tableNames, t)
		}
	}
	ignoreTables := make(map[string]bool)
	for _, t := range config.IgnoreTables {
		ignoreTables[t] = true
	}
	var ignoreTablePattern *regexp.Regexp
	if config.IgnoreTablePattern != "" {
		ignoreTablePattern, err = regexp.Compile(config.IgnoreTablePattern)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	tables := make([]*Table, 0, len(tableNames))
	for _, tableName := range tableNames {
		_, ignore := ignoreTables[tableName]
		if ignore {
			continue
		}
		if ignoreTablePattern != nil && ignoreTablePattern.MatchString(tableName) {
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

func ignoredColumnsBitmap(config ReaderConfig, table *mysqlschema.Table) []bool {
	bitmap := make([]bool, len(table.Columns))
	tableConfig, hasConfig := config.Config.Tables[table.Name]
	for i, column := range table.Columns {
		if contains(config.IgnoreColumns, table.Name+"."+column.Name) {
			bitmap[i] = true
		} else if column.Generated {
			bitmap[i] = true
		} else if hasConfig && contains(tableConfig.IgnoreColumns, column.Name) {
			bitmap[i] = true
		} else {
			bitmap[i] = false
		}
	}
	return bitmap
}

func loadTable(ctx context.Context, config ReaderConfig, databaseType DataSourceType, conn *sql.DB, schema, tableName string, tableConfig TableConfig) (*Table, error) {
	var err error
	var rows *sql.Rows
	// Figure out the schema name, on Vitess information_schema doesn't always match the schema name
	var internalTableSchema string
	rows, err = conn.QueryContext(ctx,
		`
		select table_schema from information_schema.tables 
		where table_name = ? and (table_schema like ? or table_schema = ?)
		`,
		tableName, fmt.Sprintf("vt_%s%%", schema), schema)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer rows.Close()
	if !rows.Next() {
		dumpErr := dumpInformationSchema(ctx, conn)
		if dumpErr != nil {
			logrus.Warnf("could not dump information_schema: %v", dumpErr)
		}
		return nil, errors.Errorf("table %v not found in information_schema", tableName)
	}
	err = rows.Scan(&internalTableSchema)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	mysqlTable, err := mysqlschema.NewTableFromSqlDB(conn, internalTableSchema, tableName)
	if err != nil {
		// Sometimes on Vitess the schema returned by the information_schema isn't correct,
		// we also try the configured schema as a fallback
		mysqlTable, err = mysqlschema.NewTableFromSqlDB(conn, schema, tableName)
	}
	if err != nil {
		return nil, errors.WithStack(err)
	}

	var columnNames []string
	var columnNamesQuoted []string
	var columnNamesCRC32 []string
	for _, column := range mysqlTable.Columns {
		columnName := column.Name
		if err != nil {
			return nil, errors.WithStack(err)
		}
		// There are duplicates with vttestserver because multiples shards run in the same mysqld
		if contains(columnNames, columnName) {
			continue
		}
		if contains(config.IgnoreColumns, tableName+"."+columnName) {
			continue
		}
		if contains(tableConfig.IgnoreColumns, columnName) {
			continue
		}
		if column.Generated {
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

	estimatedRows, err := estimatedRows(ctx, conn, mysqlTable)
	if err != nil {
		logrus.Warn(err)
	}

	if tableConfig.ChunkSize == 0 {
		tableConfig.ChunkSize = config.ChunkSize
	}
	if tableConfig.WriteBatchSize == 0 {
		tableConfig.WriteBatchSize = config.WriteBatchSize
	}

	table := &Table{
		Name:                 tableName,
		Columns:              columnNames,
		ColumnsQuoted:        columnNamesQuoted,
		CRC32Columns:         columnNamesCRC32,
		ColumnList:           strings.Join(columnNamesQuoted, ","),
		IgnoredColumnsBitmap: ignoredColumnsBitmap(config, mysqlTable),
		EstimatedRows:        estimatedRows,
		Config:               tableConfig,
		MysqlTable:           mysqlTable,
	}

	if len(mysqlTable.PKColumns) >= 1 {
		table.KeyColumns = table.Config.KeyColumns
		if len(table.KeyColumns) == 0 {
			for _, c := range mysqlTable.PKColumns {
				table.KeyColumns = append(table.KeyColumns, mysqlTable.Columns[c].Name)
			}
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

func dumpInformationSchema(ctx context.Context, conn *sql.DB) error {
	var tables []string
	rows, err := conn.QueryContext(ctx,
		` select table_schema, table_name from information_schema.tables `)
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()
	for rows.Next() {
		var tableSchema, tableName string
		err := rows.Scan(tableSchema, tableName)
		if err != nil {
			return errors.WithStack(err)
		}
		tables = append(tables, fmt.Sprintf("%s.%s", tableSchema, tableName))
	}
	if err != nil {
		return errors.WithStack(err)
	}
	logrus.Infof("all tables in information_schema: %v", strings.Join(tables, ", "))
	return nil
}

func estimatedRows(ctx context.Context, conn DBReader, mysqlTable *mysqlschema.Table) (int64, error) {
	var err error
	var rows *sql.Rows

	rows, err = conn.QueryContext(ctx,
		"select round(data_length/avg_row_length) from information_schema.tables where table_schema = ? and table_name = ?",
		mysqlTable.Schema, mysqlTable.Name)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	defer rows.Close()
	if !rows.Next() {
		return 0, errors.Errorf("could not estimate number of rows of table %v.%v", mysqlTable.Schema, mysqlTable.Name)
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
