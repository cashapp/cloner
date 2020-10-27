package clone

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/mightyguava/autotx"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Write directly writes a batch with retries and backoff
func Write(ctx context.Context, cmd *Clone, db *sql.DB, batch Batch) error {
	err := autotx.TransactWithRetry(ctx, db, autotx.RetryOptions{
		MaxRetries: cmd.WriteRetryCount,
	}, func(tx *sql.Tx) error {
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
		if !cmd.HighFidelity {
			// If we're doing a best effort clone we just give up on this batch
			log.WithField("task", "writer").
				WithField("table", batch.Table.Name).
				WithError(err).
				Warnf("failed write batch after %d times, "+
					"since this is a best effort clone we just give up", cmd.WriteRetryCount)
			return nil
		}

		log.WithField("task", "writer").
			WithField("table", batch.Table.Name).
			WithError(err).
			Warnf("failed write batch after %d times", cmd.WriteRetryCount)
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
		for i, _ := range columns {
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
