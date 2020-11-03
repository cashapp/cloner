package clone

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/mightyguava/autotx"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

func scheduleWriteBatch(ctx context.Context, cmd *Clone, g *errgroup.Group, writer *sql.DB, batch Batch) {
	g.Go(func() error {
		err := Write(ctx, cmd, writer, batch)

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return errors.WithStack(err)
			}

			// If we fail to write due to a uniqueness constraint violation
			// we'll split the batch so that we can write all the rows in the batch
			// that are not conflicting
			if strings.HasPrefix(err.Error(), "Error 1062:") {
				if len(batch.Rows) > 1 {
					batch1, batch2 := splitBatch(batch)
					scheduleWriteBatch(ctx, cmd, g, writer, batch1)
					scheduleWriteBatch(ctx, cmd, g, writer, batch2)
					return nil
				}
			}

			logger := log.WithField("task", "writer").
				WithField("table", batch.Table.Name).
				WithError(err)

			if !cmd.Consistent {
				// If we're doing a best effort clone we just give up on this batch
				logger.Warnf("failed write batch after retries and backoff, "+
					"since this is a best effort clone we just give up: %+v", err)
				return nil
			}

			logger.Errorf("failed write batch after %d times", cmd.WriteRetryCount)
			return errors.WithStack(err)
		}

		writesProcessed.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(len(batch.Rows)))
		return nil
	})
}

func splitBatch(batch Batch) (Batch, Batch) {
	rows := batch.Rows
	size := len(rows)
	if size == 1 {
		log.Fatalf("can't split batch of one: %v", batch)
	}
	rows1 := rows[0 : size/2]
	rows2 := rows[size/2 : size]
	batch1 := Batch{
		Type:  batch.Type,
		Table: batch.Table,
		Rows:  rows1,
	}
	batch2 := Batch{
		Type:  batch.Type,
		Table: batch.Table,
		Rows:  rows2,
	}
	return batch1, batch2
}

// Write directly writes a batch with retries and backoff
func Write(ctx context.Context, cmd *Clone, db *sql.DB, batch Batch) error {
	err := autotx.TransactWithRetry(ctx, db, autotx.RetryOptions{
		MaxRetries: cmd.WriteRetryCount,
		BackOff:    newSimpleExponentialBackOff(5 * time.Minute).NextBackOff,
	}, func(tx *sql.Tx) error {
		ctx, cancel := context.WithTimeout(ctx, cmd.WriteTimeout)
		defer cancel()

		switch batch.Type {
		case Insert:
			return insertBatch(ctx, tx, batch)
		case Delete:
			return deleteBatch(ctx, tx, batch)
		case Update:
			return updateBatch(ctx, tx, batch)
		default:
			log.Panicf("Unknown batch type %s", batch.Type)
			return nil
		}
	})

	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func deleteBatch(ctx context.Context, tx *sql.Tx, batch Batch) error {
	rows := batch.Rows
	log.Debugf("deleting %d rows", len(rows))

	table := batch.Table
	questionMarks := make([]string, 0, len(rows))
	for range rows {
		questionMarks = append(questionMarks, "?")
	}

	valueArgs := make([]interface{}, 0, len(rows))
	for _, post := range rows {
		valueArgs = append(valueArgs, post.ID)
	}
	stmt := fmt.Sprintf("DELETE FROM %s WHERE %s IN (%s)",
		table.Name, table.IDColumn, strings.Join(questionMarks, ","))
	_, err := tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}
	return nil
}

func insertBatch(ctx context.Context, tx *sql.Tx, batch Batch) error {
	log.Debugf("inserting %d rows", len(batch.Rows))

	table := batch.Table
	columns := table.Columns
	questionMarks := make([]string, 0, len(columns))
	for range columns {
		questionMarks = append(questionMarks, "?")
	}
	values := fmt.Sprintf("(%s)", strings.Join(questionMarks, ","))

	rows := batch.Rows
	valueStrings := make([]string, 0, len(rows))
	valueArgs := make([]interface{}, 0, len(rows)*len(columns))
	for _, row := range rows {
		valueStrings = append(valueStrings, values)
		for i := range columns {
			valueArgs = append(valueArgs, row.Data[i])
		}
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table.Name, table.ColumnList, strings.Join(valueStrings, ","))
	_, err := tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}
	return nil
}

func updateBatch(ctx context.Context, tx *sql.Tx, batch Batch) error {
	rows := batch.Rows
	log.Debugf("updating %d rows", len(rows))

	table := batch.Table
	columns := table.Columns
	columnValues := make([]string, 0, len(columns))

	for _, column := range columns {
		if column != table.IDColumn {
			c := fmt.Sprintf("`%s` = ?", column)
			columnValues = append(columnValues, c)
		}
	}

	stmt := fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` = ?",
		table.Name, strings.Join(columnValues, ","), table.IDColumn)
	prepared, err := tx.PrepareContext(ctx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not prepare: %s", stmt)
	}

	for _, row := range rows {
		args := make([]interface{}, 0, len(columns))
		for i, column := range columns {
			if column != table.IDColumn {
				args = append(args, row.Data[i])
			}
		}
		args = append(args, row.ID)

		_, err = prepared.ExecContext(ctx, args...)
		if err != nil {
			return errors.Wrapf(err, "could not execute: %s", stmt)
		}
	}
	return nil
}
