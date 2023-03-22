package clone

import (
	"context"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

type Ping struct {
	SourceTargetConfig

	Table string `help:"If set select a row from this table" optional:""`
}

func (cmd *Ping) Run() error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), time.Second*5)
	defer cancelFunc()

	if cmd.Source.Host != "" || cmd.Source.MiskDatasource != "" {
		log.Infof("pinging source")
		err := cmd.pingDatabase(ctx, cmd.Source)
		if err != nil {
			return errors.WithStack(err)
		}
		log.Infof("success")
	}

	if cmd.Target.Host != "" || cmd.Target.MiskDatasource != "" {
		log.Infof("pinging target")
		err := cmd.pingDatabase(ctx, cmd.Target)
		if err != nil {
			return errors.WithStack(err)
		}
		log.Infof("success")
	}

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

	err = tx.Rollback()
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
