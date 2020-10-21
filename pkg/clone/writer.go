package clone

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

var (
	writesProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writes_processed",
			Help: "How many writes, partitioned by table and type (insert, update, delete).",
		},
		[]string{"table", "type"},
	)
)

func init() {
	prometheus.MustRegister(writesProcessed)
}

func Write(ctx context.Context, conn *sql.Conn, batches chan Batch) error {
	for {
		select {
		case batch, more := <-batches:
			if !more {
				return nil
			}
			err := writeBatch(ctx, conn, batch)
			if err != nil {
				log.WithField("task", "writer").
					WithField("table", batch.Table.Name).
					WithError(err).
					Errorf("")
				return errors.WithStack(err)
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
	_, err := conn.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}
	writesProcessed.WithLabelValues(batch.Table.Name, "delete").Add(float64(len(rows)))
	return nil
}

func insertBatch(ctx context.Context, conn *sql.Conn, batch Batch) error {
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
	for _, post := range rows {
		valueStrings = append(valueStrings, values)
		for i, _ := range columns {
			valueArgs = append(valueArgs, post.Data[i])
		}
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table.Name, table.ColumnList, strings.Join(valueStrings, ","))
	_, err := conn.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}
	writesProcessed.WithLabelValues(batch.Table.Name, "insert").Add(float64(len(rows)))
	return nil
}

func updateBatch(ctx context.Context, conn *sql.Conn, batch Batch) error {
	rows := batch.Rows
	log.Debugf("updating %d rows", len(rows))

	// Use a transaction to do some limited level of batching
	// Golang doesn't yet support "real" batching of multiple statements
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	err = updatedBatchInTx(ctx, tx, batch)

	if err != nil {
		_ = tx.Rollback()
		return err
	} else {
		err := tx.Commit()
		writesProcessed.WithLabelValues(batch.Table.Name, "update").Add(float64(len(rows)))
		return err
	}
}

func updatedBatchInTx(ctx context.Context, tx *sql.Tx, batch Batch) error {
	rows := batch.Rows
	table := batch.Table
	columns := table.Columns
	columnValues := make([]string, 0, len(columns))

	for _, column := range columns {
		if column != table.IDColumn {
			c := fmt.Sprintf("%s = ?", column)
			columnValues = append(columnValues, c)
		}
	}

	stmt := fmt.Sprintf("UPDATE %s SET %s WHERE %s = ?",
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
