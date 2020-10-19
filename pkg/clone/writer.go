package clone

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

func Write(ctx context.Context, conn *sql.Conn, batches chan Batch) error {
	log.Debugf("Starting writer")
	for {
		select {
		case batch, more := <-batches:
			if more {
				err := writeBatch(ctx, conn, batch)
				if err != nil {
					return errors.WithStack(err)
				}
			} else {
				log.Debugf("Writer done!")
				return nil
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func writeBatch(ctx context.Context, conn *sql.Conn, batch Batch) error {
	// TODO retries and backoff
	switch batch.Type {
	case Insert:
		return insertBatch(ctx, conn, batch)
	case Delete:
		return deleteBatch(ctx, conn, batch)
	case Update:
		return updateBatch(ctx, conn, batch)
	default:
		return errors.Errorf("Unknown batch type %s", batch.Type)
	}
}

func deleteBatch(ctx context.Context, conn *sql.Conn, batch Batch) error {
	rows := batch.Rows
	log.Debugf("Deleting %d rows", len(rows))

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
	_, err := conn.ExecContext(ctx, stmt, valueArgs...)
	return err
}

func insertBatch(ctx context.Context, conn *sql.Conn, batch Batch) error {
	log.Debugf("Inserting %d rows", len(batch.Rows))

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
	for _, post := range rows {
		valueStrings = append(valueStrings, values)
		for i, _ := range columns {
			valueArgs = append(valueArgs, post.Data[i])
		}
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table.Name, strings.Join(table.Columns, ","), strings.Join(valueStrings, ","))
	_, err := conn.ExecContext(ctx, stmt, valueArgs...)
	return err
}

func updateBatch(ctx context.Context, conn *sql.Conn, batch Batch) error {
	rows := batch.Rows
	log.Debugf("Updating %d rows", len(rows))

	// Use a transaction to do some limited level of batching
	// Golang doesn't yet support "real" batching of multiple statements
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	table := batch.Table
	columns := table.Columns
	columnValues := make([]string, 0, len(columns))

	for _, column := range columns {
		if column != table.IDColumn {
			c := fmt.Sprintf("%s = ?", column)
			columnValues = append(columnValues, c)
		}
	}

	for _, row := range rows {
		args := make([]interface{}, 0, len(columns))
		for i, column := range columns {
			if column != table.IDColumn {
				args = append(args, row.Data[i])
			}
		}
		args = append(args, row.ID)

		stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
			table.Name, strings.Join(columnValues, ","), table.IDColumn)
		_, err = tx.ExecContext(ctx, stmt, args)
		if err != nil {
			break
		}
	}

	if err != nil {
		_ = tx.Rollback()
		return err
	} else {
		return tx.Commit()
	}
}
