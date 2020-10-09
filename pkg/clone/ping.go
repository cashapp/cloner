package clone

import (
	"context"
	"fmt"
	"time"

	log "github.com/sirupsen/logrus" // nolint: depguard
)

type Ping struct {
	Table string `help:"If set select a row from this table" optional:""`
}

func (cmd *Ping) Run(globals Globals) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelFunc()
	sourceDB, err := globals.Source.DB()
	if err != nil {
		return err
	}
	err = sourceDB.PingContext(ctx)
	if err != nil {
		return err
	}
	tx, err := sourceDB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	row := tx.QueryRowContext(ctx, "SELECT NOW()")
	var now time.Time
	err = row.Scan(now)
	if err != nil {
		return err
	}

	tableRow := tx.QueryRowContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 1", cmd.Table))
	var tableData []interface{}
	err = tableRow.Scan(tableData...)
	if err != nil {
		return err
	}
	log.Infof("successfully pinged source (now = %v, row = %v)", now, tableData)
	// TODO ping target
	return nil
}
