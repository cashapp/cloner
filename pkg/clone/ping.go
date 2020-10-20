package clone

import (
	"context"
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

	log.Infof("pinging source")
	err := cmd.pingDatabase(ctx, globals.Source)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("success")

	log.Infof("pinging target")
	err = cmd.pingDatabase(ctx, globals.Target)
	if err != nil {
		return errors.WithStack(err)
	}
	log.Infof("success")

	return nil
}

func (cmd *Ping) pingDatabase(ctx context.Context, config DBConfig) error {
	db, err := config.DB()
	if err != nil {
		return errors.WithStack(err)
	}

	err = db.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	tx, err := db.BeginTx(ctx, nil)
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
	return nil
}
