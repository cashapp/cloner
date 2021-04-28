package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/dlmiddlecote/sqlstats"
	"github.com/go-sql-driver/mysql"
	"golang.org/x/sync/semaphore"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/mightyguava/autotx"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	writesRequested = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writes_requested",
			Help: "How many writes (rows) have been requested, partitioned by table and type (insert, update, delete).",
		},
		[]string{"table", "type"},
	)
	writesRowsAffected = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writes_rows_affected",
			Help: "How many \"rows affected\" that have been returned from executed batch write statements, " +
				"partitioned by table and type (insert, update, delete).",
		},
		[]string{"table", "type"},
	)
	writesSucceeded = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writes_succeeded",
			Help: "How many writes (rows) have succeeded, partitioned by table and type (insert, update, delete).",
		},
		[]string{"table", "type"},
	)
	writesFailed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writes_errors",
			Help: "How many writes (rows) have failed irrecoverably, partitioned by table and type (insert, update, delete).",
		},
		[]string{"table", "type"},
	)
	writeDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "write_duration",
			Help:       "Duration of writes (does not include retries and backoff).",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"table", "type"},
	)
	writeLimiterDelay = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name: "writer_limiter_delay_duration",
			Help: "Duration of back off from the concurrency limiter.",
		},
	)
)

func init() {
	prometheus.MustRegister(writesRequested)
	prometheus.MustRegister(writesRowsAffected)
	prometheus.MustRegister(writesSucceeded)
	prometheus.MustRegister(writesFailed)
	prometheus.MustRegister(writeDuration)
	prometheus.MustRegister(writeLimiterDelay)
}

func (w *Writer) scheduleWriteBatch(ctx context.Context, g *errgroup.Group, batch Batch) (err error) {
	err = w.writerParallelism.Acquire(ctx, 1)
	if err != nil {
		return errors.WithStack(err)
	}
	writesRequested.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(len(batch.Rows)))
	g.Go(func() (err error) {
		defer w.writerParallelism.Release(1)
		err = w.writeBatch(ctx, batch)

		if err != nil {
			if errors.Is(err, context.Canceled) {
				return errors.WithStack(err)
			}

			// If we fail to write we'll split the batch
			// * It could be because of a conflict violation in which case we want to write all the non-conflicting rows
			// * It could be because some rows in the batch are too big in which case we want to decrease the batch size
			// In either case splitting the batch is better
			if len(batch.Rows) > 1 {
				batch1, batch2 := splitBatch(batch)
				err = w.scheduleWriteBatch(ctx, g, batch1)
				if err != nil {
					return errors.WithStack(err)
				}
				err := w.scheduleWriteBatch(ctx, g, batch2)
				if err != nil {
					return errors.WithStack(err)
				}
				return nil
			}

			if !w.config.Consistent {
				logger := log.WithField("table", batch.Table.Name).WithError(err)
				// If we're doing a best effort clone we just give up on this batch
				logger.Warnf("failed write batch after retries and backoff, "+
					"since this is a best effort clone we just give up: %+v", err)
				return nil
			}

			return errors.WithStack(err)
		}
		return nil
	})
	return nil
}

// mysqlError returns a mysql.MySQLError if there is such in the causal chain, if not returns nil
func mysqlError(err error) *mysql.MySQLError {
	if err == nil {
		return nil
	}
	me, ok := err.(*mysql.MySQLError)
	if ok {
		return me
	}
	cause := errors.Cause(err)
	if cause == err {
		// errors.Cause returns the error if there is no cause
		return nil
	} else {
		return mysqlError(cause)
	}
}

// isConstraintViolation are errors caused by a single row in a batch, for these errors we break down the batch
// into smaller parts until we find the erroneous row, uniqueness constraints can also be fixed by clone jobs for other
// shards removing the row if the row has been copied from one shard to the other
//nolint:deadcode,unused
func isConstraintViolation(err error) bool {
	me := mysqlError(err)
	if me == nil {
		return false
	}
	switch me.Number {
	// Various constraint violations
	case 1022, 1048, 1052, 1062, 1169, 1216, 1217, 1451, 1452, 1557, 1586, 1761, 1762, 1859:
		return true
	// Error 1292: Incorrect timestamp value: '0000-00-00'
	case 1292:
		return true
	default:
		return false
	}
}

// isSchemaError are errors that should immediately fail the clone operation and can't be fixed by retrying
func isSchemaError(err error) bool {
	me := mysqlError(err)
	if me == nil {
		return false
	}
	switch me.Number {
	// Error 1146: Table does not exist
	// TODO we should also check for unknown column
	case 1146:
		return true
	default:
		return false
	}
}

func splitBatch(batch Batch) (Batch, Batch) {
	rows := batch.Rows
	size := len(rows)
	if size == 0 {
		log.Fatalf("can't split empty batch: %v", batch)
	}
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
	if len(batch1.Rows) >= size {
		log.Fatalf("splitBatch didn't shrink batch: %v", batch1)
	}
	if len(batch2.Rows) >= size {
		log.Fatalf("splitBatch didn't shrink batch: %v", batch1)
	}
	return batch1, batch2
}

func (w *Writer) writeBatch(ctx context.Context, batch Batch) (err error) {
	logger := log.WithField("task", "writer").WithField("table", batch.Table.Name)

	err = Retry(ctx, w.retry, func(ctx context.Context) error {
		err = autotx.Transact(ctx, w.db, func(tx *sql.Tx) error {
			timer := prometheus.NewTimer(writeDuration.WithLabelValues(batch.Table.Name, string(batch.Type)))
			defer timer.ObserveDuration()
			defer func() {
				if err == nil {
					writesSucceeded.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(len(batch.Rows)))
				} else {
					writesFailed.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(len(batch.Rows)))
				}
			}()

			if w.config.NoDiff {
				return w.replaceBatch(ctx, logger, tx, batch)
			} else {
				switch batch.Type {
				case Insert:
					return w.insertBatch(ctx, logger, tx, batch)
				case Delete:
					return w.deleteBatch(ctx, logger, tx, batch)
				case Update:
					return w.updateBatch(ctx, logger, tx, batch)
				default:
					logger.Panicf("Unknown batch type %s", batch.Type)
					return nil
				}
			}
		})

		// These should not be retried
		if isSchemaError(err) {
			return backoff.Permanent(err)
		}
		// We immediately fail non-single row batches, the caller will do binary chop to find the violating row
		if len(batch.Rows) > 1 {
			return backoff.Permanent(err)
		}

		if err != nil {
			return errors.WithStack(err)
		}

		return nil

	})

	return errors.WithStack(err)
}

func (w *Writer) deleteBatch(ctx context.Context, logger *log.Entry, tx *sql.Tx, batch Batch) error {
	logger = logger.WithField("op", "delete")
	rows := batch.Rows
	logger.Debugf("deleting %d rows", len(rows))

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
	result, err := tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}
	rowsAffected, err := result.RowsAffected()
	// If we get an error we'll just ignore that...
	if err != nil {
		writesRowsAffected.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(rowsAffected))
	}
	return nil
}

func (w *Writer) replaceBatch(ctx context.Context, logger *log.Entry, tx *sql.Tx, batch Batch) error {
	if batch.Type != Insert {
		return fmt.Errorf("this method only handles inserts")
	}
	logger = logger.WithField("op", "insert")
	logger.Debugf("inserting %d rows", len(batch.Rows))

	table := batch.Table
	columns := table.Columns
	questionMarks := make([]string, 0, len(columns))
	for range columns {
		questionMarks = append(questionMarks, "?")
	}
	values := fmt.Sprintf("(%s)", strings.Join(questionMarks, ","))

	statementBatches := batches(batch.Rows, w.config.WriteBatchStatementSize)
	for _, statementBatch := range statementBatches {
		valueStrings := make([]string, 0, len(statementBatch))
		valueArgs := make([]interface{}, 0, len(statementBatch)*len(columns))
		for _, row := range statementBatch {
			valueStrings = append(valueStrings, values)
			for i := range columns {
				valueArgs = append(valueArgs, row.Data[i])
			}
		}
		//stmt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
		//	table.Name, table.ColumnList, strings.Join(valueStrings, ","))
		stmt := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
			table.Name, table.ColumnList, strings.Join(valueStrings, ","))
		result, err := tx.ExecContext(ctx, stmt, valueArgs...)
		if err != nil {
			return errors.Wrapf(err, "could not execute: %s", stmt)
		}
		fmt.Println("INSERT IGNORE!!")
		rowsAffected, err := result.RowsAffected()
		// If we get an error we'll just ignore that...
		if err != nil {
			writesRowsAffected.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(rowsAffected))
		}
	}

	return nil
}

func batches(rows []*Row, limit int) [][]*Row {
	size := len(rows)
	batches := make([][]*Row, 0, size/limit)
	for i := 0; i < size; i += limit {
		batches = append(batches, rows[i:min(i+limit, size)])
	}
	return batches
}

func min(a, b int) int {
	if a <= b {
		return a
	}
	return b
}

func (w *Writer) insertBatch(ctx context.Context, logger *log.Entry, tx *sql.Tx, batch Batch) error {
	logger = logger.WithField("op", "insert")
	logger.Debugf("inserting %d rows", len(batch.Rows))

	table := batch.Table
	columns := table.Columns
	questionMarks := make([]string, 0, len(columns))
	for range columns {
		questionMarks = append(questionMarks, "?")
	}
	values := fmt.Sprintf("(%s)", strings.Join(questionMarks, ","))

	statementBatches := batches(batch.Rows, w.config.WriteBatchStatementSize)
	for _, statementBatch := range statementBatches {
		valueStrings := make([]string, 0, len(statementBatch))
		valueArgs := make([]interface{}, 0, len(statementBatch)*len(columns))
		for _, row := range statementBatch {
			valueStrings = append(valueStrings, values)
			for i := range columns {
				valueArgs = append(valueArgs, row.Data[i])
			}
		}
		stmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES %s",
			table.Name, table.ColumnList, strings.Join(valueStrings, ","))
		result, err := tx.ExecContext(ctx, stmt, valueArgs...)
		if err != nil {
			return errors.Wrapf(err, "could not execute: %s", stmt)
		}
		rowsAffected, err := result.RowsAffected()
		if err != nil {
			// If we get an error we'll just ignore that...
			writesRowsAffected.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(rowsAffected))
		}
	}
	return nil
}

func (w *Writer) updateBatch(ctx context.Context, logger *log.Entry, tx *sql.Tx, batch Batch) error {
	logger = logger.WithField("op", "update")
	rows := batch.Rows
	logger.Debugf("updating %d rows", len(rows))

	table := batch.Table
	columns := table.Columns
	columnValues := make([]string, 0, len(columns))

	for _, column := range columns {
		if column != table.IDColumn {
			c := fmt.Sprintf("`%s` = ?", column)
			columnValues = append(columnValues, c)
		}
	}

	// We don't use batch statements for updates, instead we prepare a single statement and do all the updates
	// in the same transaction

	stmt := fmt.Sprintf("UPDATE `%s` SET %s WHERE `%s` = ?",
		table.Name, strings.Join(columnValues, ","), table.IDColumn)
	prepared, err := tx.PrepareContext(ctx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not prepare: %s", stmt)
	}
	defer prepared.Close()

	for _, row := range rows {
		args := make([]interface{}, 0, len(columns))
		for i, column := range columns {
			if column != table.IDColumn {
				args = append(args, row.Data[i])
			}
		}
		args = append(args, row.ID)

		result, err := prepared.ExecContext(ctx, args...)
		if err != nil {
			return errors.Wrapf(err, "could not execute: %s", stmt)
		}
		rowsAffected, err := result.RowsAffected()
		// If we get an error we'll just ignore that...
		if err != nil {
			writesRowsAffected.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(rowsAffected))
		}
	}
	return nil
}

type Writer struct {
	config WriterConfig

	db    *sql.DB
	retry RetryOptions

	writerParallelism *semaphore.Weighted
}

func NewWriter(config WriterConfig) *Writer {
	w := &Writer{config: config}
	w.retry = RetryOptions{
		Limiter:       makeLimiter("writer_limiter"),
		AcquireMetric: writeLimiterDelay,
		MaxRetries:    w.config.WriteRetries,
		Timeout:       w.config.WriteTimeout,
	}
	w.writerParallelism = semaphore.NewWeighted(w.config.WriterParallelism)
	return w
}

func (w *Writer) Write(ctx context.Context, g *errgroup.Group, diffs chan Diff) error {
	writer, err := w.config.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	w.db = writer
	// Refresh connections regularly so they don't go stale
	writer.SetConnMaxLifetime(time.Minute)
	writer.SetMaxOpenConns(w.config.WriterCount)
	writerCollector := sqlstats.NewStatsCollector("target_writer", writer)
	prometheus.MustRegister(writerCollector)
	defer prometheus.Unregister(writerCollector)

	// Batch up the diffs
	batches := make(chan Batch)
	g.Go(func() error {
		err := BatchWrites(ctx, diffs, batches)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	// Write every batch
	g.Go(func() error {
		g, ctx := errgroup.WithContext(ctx)
		for batch := range batches {
			err := w.scheduleWriteBatch(ctx, g, batch)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		err := g.Wait()
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	return nil
}

func (w *Writer) Close() error {
	return w.db.Close()
}
