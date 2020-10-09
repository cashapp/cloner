package clone

import "database/sql"

type Table struct {
	Name     string
	IDColumn string
	Columns  []string
}

func LoadTables(conn *sql.Conn) ([]*Table, error) {
	// TODO
	return nil, nil
}
