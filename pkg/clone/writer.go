package clone

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"

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

func Write(ctx context.Context, cmd *Clone, db *sql.DB, writeRequests chan Batch, writesInFlight *sync.WaitGroup) error {
	for {
		select {
		case batch, more := <-writeRequests:
			if !more {
				return nil
			}
			// TODO backoff
			err := writeBatch(ctx, db, batch)
			if err != nil {
				err := maybeRetry(ctx, cmd.WriteRetryCount, err, batch, writeRequests)
				if err != nil {
					if !cmd.HighFidelity {
						// If we're doing a best effort clone we just give up on this batch
						log.WithField("task", "writer").
							WithField("table", batch.Table.Name).
							WithError(err).
							Warnf("since this is a best effort clone we just give up")
						continue
					} else {
						log.WithField("task", "writer").
							WithField("table", batch.Table.Name).
							WithError(err).
							Errorf("")
						return errors.WithStack(err)
					}
				}
				continue
			}
			writesInFlight.Done()
		case <-ctx.Done():
			return nil
		}
	}
}

func maybeRetry(ctx context.Context, retryCount int, err error, batch Batch, retries chan Batch) error {
	if batch.Retries >= retryCount {
		return err
	}

	log.WithField("task", "writer").
		WithField("table", batch.Table.Name).
		WithError(err).
		Warnf("failed to write batch, retrying %d times", retryCount-batch.Retries)

	batch.Retries += 1
	batch.LastError = err

	// Check if the context has been cancelled before we retry
	// otherwise we might post on a closed channel
	select {
	case <-ctx.Done():
		return nil
	default:
	}
	retries <- batch
	return nil
}

func writeBatch(ctx context.Context, db *sql.DB, batch Batch) error {
	switch batch.Type {
	case Insert:
		return insertBatch(ctx, db, batch)
	case Delete:
		return deleteBatch(ctx, db, batch)
	case Update:
		return updateBatch(ctx, db, batch)
	default:
		log.Panicf("Unknown batch type %s", batch.Type)
		return nil
	}
}

func deleteBatch(ctx context.Context, db *sql.DB, batch Batch) error {
	rows := batch.Rows
	log.Debugf("deleting %d rows", len(rows))

	//for _, row := range batch.Rows {
	//	log.WithField("task", "writer").
	//		WithField("table", batch.Table.Name).
	//		Infof("NOT deleting %d", row.ID)
	//}

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
	_, err := db.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}
	writesProcessed.WithLabelValues(batch.Table.Name, "delete").Add(float64(len(rows)))
	return nil
}

func insertBatch(ctx context.Context, db *sql.DB, batch Batch) error {
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
		if row.ID == 1016 {
			log.Debugf("inserting 1016!")
		}
		valueStrings = append(valueStrings, values)
		for i, _ := range columns {
			valueArgs = append(valueArgs, row.Data[i])
		}
	}
	stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
		table.Name, table.ColumnList, strings.Join(valueStrings, ","))
	_, err := db.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}
	writesProcessed.WithLabelValues(batch.Table.Name, "insert").Add(float64(len(rows)))
	return nil
}

func updateBatch(ctx context.Context, db *sql.DB, batch Batch) error {
	rows := batch.Rows
	log.Debugf("updating %d rows", len(rows))

	// Use a transaction to do some limited level of batching
	// Golang doesn't yet support "real" batching of multiple statements
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return errors.WithStack(err)
	}

	err = updatedBatchInTx(ctx, tx, batch)

	if err != nil {
		_ = tx.Rollback()
		return errors.WithStack(err)
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
