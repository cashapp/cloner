package clone

import (
	"context"
	"fmt"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Ping struct {
	Table string `help:"If set select a row from this table" optional:""`
}

func (cmd *Ping) Run(globals Globals) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelFunc()
	sourceDB, err := globals.Source.DB()
	if err != nil {
		return errors.WithStack(err)
	}

	err = sourceDB.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	tx, err := sourceDB.BeginTx(ctx, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	var now time.Time
	{
		row := tx.QueryRowContext(ctx, "SELECT NOW()")
		err = row.Scan(&now)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	var row []interface{}
	if cmd.Table != "" {
		rows, err := tx.QueryContext(ctx, fmt.Sprintf("SELECT * FROM %s LIMIT 1", cmd.Table))
		if err != nil {
			return errors.WithStack(err)
		}
		cols, err := rows.Columns()
		if err != nil {
			return errors.WithStack(err)
		}
		row = make([]interface{}, len(cols))
		for rows.Next() {
			for i, _ := range row {
				row[i] = new(interface{})
			}
			err = rows.Scan(row...)
			if err != nil {
				return errors.WithStack(err)
			}
		}
	}

	log.Infof("successfully pinged source (now = %v, row = %v)", now, row)

	// TODO ping target
	return nil
}
