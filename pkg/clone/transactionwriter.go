package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/mightyguava/autotx"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	_ "net/http/pprof"
	"strings"
	"time"
)

// TransactionWriter receives transactions and requests to snapshot and writes transactions and strongly consistent chunk snapshots
type TransactionWriter struct {
	config       Replicate
	source       *sql.DB
	sourceSchema string
	target       *sql.DB

	sourceRetry RetryOptions
	targetRetry RetryOptions
}

func NewTransactionWriter(config Replicate) (*TransactionWriter, error) {
	var err error
	r := TransactionWriter{
		config: config,
		sourceRetry: RetryOptions{
			Limiter:       nil, // will we ever use concurrency limiter again? probably not?
			AcquireMetric: readLimiterDelay.WithLabelValues("source"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
		targetRetry: RetryOptions{
			Limiter:       nil, // will we ever use concurrency limiter again? probably not?
			AcquireMetric: readLimiterDelay.WithLabelValues("target"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
	}
	target, err := r.config.Target.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.target = target

	return &r, nil
}

func (w *TransactionWriter) Init(ctx context.Context) error {
	err := w.target.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	if w.config.CreateTables {
		err = w.createCheckpointTable(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (w *TransactionWriter) Run(ctx context.Context, b backoff.BackOff, transactions chan Transaction) error {
	for {
		var transaction Transaction
		select {
		case transaction = <-transactions:
		case <-ctx.Done():
			return ctx.Err()
		}

		err := autotx.Transact(ctx, w.target, func(tx *sql.Tx) error {
			for _, mutation := range transaction.Mutations {
				err := w.handleMutation(ctx, tx, mutation)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			err := w.writeCheckpoint(ctx, tx, transaction.FinalPosition)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}

		// We've committed a transaction, we can reset the backoff
		b.Reset()
	}
}

func (w *TransactionWriter) handleMutation(ctx context.Context, tx *sql.Tx, mutation Mutation) error {
	if mutation.Type == Delete {
		err := w.deleteRows(ctx, tx, mutation)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		err := w.replaceRows(ctx, tx, mutation)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (w *TransactionWriter) replaceRows(ctx context.Context, tx *sql.Tx, mutation Mutation) error {
	var err error
	tableSchema := mutation.Table.MysqlTable
	tableName := tableSchema.Name
	writeType := mutation.Type.String()
	timer := prometheus.NewTimer(writeDuration.WithLabelValues(tableName, writeType))
	defer timer.ObserveDuration()
	defer func() {
		if err == nil {
			writesSucceeded.WithLabelValues(tableName, writeType).Add(float64(len(mutation.Rows)))
		} else {
			writesFailed.WithLabelValues(tableName, writeType).Add(float64(len(mutation.Rows)))
		}
	}()
	var questionMarks strings.Builder
	var columnListBuilder strings.Builder
	for i, column := range tableSchema.Columns {
		questionMarks.WriteString("?")
		columnListBuilder.WriteString("`")
		columnListBuilder.WriteString(column.Name)
		columnListBuilder.WriteString("`")
		if i != len(tableSchema.Columns)-1 {
			columnListBuilder.WriteString(",")
			questionMarks.WriteString(",")
		}
	}
	values := fmt.Sprintf("(%s)", questionMarks.String())
	columnList := columnListBuilder.String()

	valueStrings := make([]string, 0, len(mutation.Rows))
	valueArgs := make([]interface{}, 0, len(mutation.Rows)*len(tableSchema.Columns))
	for _, row := range mutation.Rows {
		valueStrings = append(valueStrings, values)
		valueArgs = append(valueArgs, row...)
	}
	// TODO build the entire statement with a strings.Builder like in deleteRows below. For speed.
	stmt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
		tableSchema.Name, columnList, strings.Join(valueStrings, ","))
	_, err = tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}

	return nil
}

func (w *TransactionWriter) deleteRows(ctx context.Context, tx *sql.Tx, mutation Mutation) (err error) {
	tableSchema := mutation.Table.MysqlTable
	tableName := tableSchema.Name
	writeType := mutation.Type.String()
	timer := prometheus.NewTimer(writeDuration.WithLabelValues(tableName, writeType))
	defer timer.ObserveDuration()
	defer func() {
		if err == nil {
			writesSucceeded.WithLabelValues(tableName, writeType).Add(float64(len(mutation.Rows)))
		} else {
			writesFailed.WithLabelValues(tableName, writeType).Add(float64(len(mutation.Rows)))
		}
	}()
	var stmt strings.Builder
	args := make([]interface{}, 0, len(mutation.Rows))
	stmt.WriteString("DELETE FROM `")
	stmt.WriteString(mutation.Table.Name)
	stmt.WriteString("` WHERE ")
	for rowIdx, row := range mutation.Rows {
		stmt.WriteString("(")
		for i, pkIndex := range tableSchema.PKColumns {
			args = append(args, row[pkIndex])

			stmt.WriteString("`")
			stmt.WriteString(tableSchema.Columns[pkIndex].Name)
			stmt.WriteString("` = ?")
			if i != len(tableSchema.PKColumns)-1 {
				stmt.WriteString(" AND ")
			}
		}
		stmt.WriteString(")")

		if rowIdx != len(mutation.Rows)-1 {
			stmt.WriteString(" OR ")
		}
	}

	stmtString := stmt.String()
	_, err = tx.ExecContext(ctx, stmtString, args...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmtString)
	}
	return nil
}

func (w *TransactionWriter) createCheckpointTable(ctx context.Context) error {
	// TODO retries with backoff?
	timeoutCtx, cancel := context.WithTimeout(ctx, w.config.WriteTimeout)
	defer cancel()
	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			name      VARCHAR(255) NOT NULL,
			file      VARCHAR(255) NOT NULL,
			position  BIGINT(20)   NOT NULL,
			gtid_set  TEXT,
			timestamp TIMESTAMP    NOT NULL,
			PRIMARY KEY (name)
		)
		`, w.config.CheckpointTable)
	_, err := w.target.ExecContext(timeoutCtx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not create checkpoint table in target database:\n%s", stmt)
	}
	return nil
}

func (w *TransactionWriter) writeCheckpoint(ctx context.Context, tx *sql.Tx, position Position) error {
	gsetString := ""
	if position.Gset != nil {
		gsetString = position.Gset.String()
	}
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf("REPLACE INTO %s (name, file, position, gtid_set, timestamp) VALUES (?, ?, ?, ?, ?)",
			w.config.CheckpointTable),
		w.config.TaskName, position.File, position.Position, gsetString, time.Now().UTC())
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
