package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/go-sql-driver/mysql"
	"github.com/mightyguava/autotx"
	"github.com/pkg/errors"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"strconv"
	"strings"
)

var (
	// defaultBuckets suitable for database operations, the upper buckets are of course defects but useful for debugging
	defaultBuckets = []float64{.01, .05, .1, .5, 1, 2.5, 5, 10, 60, 120, 300}

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
		[]string{"table", "type", "code"},
	)
	constraintViolationErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "constraint_violation_errors",
			Help: "How many constraint violations we're causing (this is increased once per row after retries)",
		},
		[]string{"table", "type"},
	)
	schemaErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "schema_errors",
			Help: "How many schema errors we're getting (this is increased once per row after retries)",
		},
		[]string{"table", "type"},
	)
	writeDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "write_duration",
			Help:    "Duration of writes (does not include retries and backoff).",
			Buckets: defaultBuckets,
		},
		[]string{"table", "type"},
	)
	writeLimiterDelay = prometheus.NewHistogram(
		prometheus.HistogramOpts{
			Name:    "writer_limiter_delay_duration",
			Help:    "Duration of back off from the concurrency limiter.",
			Buckets: defaultBuckets,
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
				// Schedule in separate go routines so that we release the semaphore, otherwise we can get into a
				// deadlock where we're waiting for the currently held semaphore to be released in order for these two
				// scheduleWriteBatch calls to return which means we deadlock here
				g.Go(func() error {
					err = w.scheduleWriteBatch(ctx, g, batch1)
					if err != nil {
						return errors.WithStack(err)
					}
					return nil
				})
				g.Go(func() error {
					err := w.scheduleWriteBatch(ctx, g, batch2)
					if err != nil {
						return errors.WithStack(err)
					}
					return nil
				})
				return nil
			}

			if isConstraintViolation(err) {
				constraintViolationErrors.WithLabelValues(batch.Table.Name, batch.Type.String()).Inc()
			}
			if isSchemaError(err) {
				schemaErrors.WithLabelValues(batch.Table.Name, batch.Type.String()).Inc()
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

// isWriteConflict are errors caused by a write-write conflict between two parallel transactions:
//   https://docs.pingcap.com/tidb/stable/error-codes
//nolint:deadcode,unused
func isWriteConflict(err error) bool {
	me := mysqlError(err)
	if me == nil {
		return false
	}
	return me.Number == 9007
}

// isSchemaError are errors that should immediately fail the clone operation and can't be fixed by retrying
func isSchemaError(err error) bool {
	me := mysqlError(err)
	if me == nil {
		return false
	}
	switch me.Number {
	// Error 1146: Table does not exist
	// Error 1054: Unknown column
	case 1146, 1054:
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

	retry := w.retry
	timout := batch.Table.Config.WriteTimout.Duration
	if timout != 0 {
		retry.Timeout = timout
	}
	err = Retry(ctx, retry, func(ctx context.Context) error {
		err = autotx.Transact(ctx, w.db, func(tx *sql.Tx) error {
			timer := prometheus.NewTimer(writeDuration.WithLabelValues(batch.Table.Name, string(batch.Type)))
			defer timer.ObserveDuration()
			defer func() {
				if err == nil {
					writesSucceeded.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(len(batch.Rows)))
				} else {
					mySQLError := mysqlError(err)
					var errorCode uint16
					if mySQLError != nil {
						errorCode = mySQLError.Number
					}
					writesFailed.WithLabelValues(batch.Table.Name, string(batch.Type), strconv.Itoa(int(errorCode))).
						Add(float64(len(batch.Rows)))
				}
			}()

			if w.config.NoDiff {
				err = w.replaceBatch(ctx, logger, tx, batch)
			} else {
				switch batch.Type {
				case Insert:
					err = w.insertBatch(ctx, logger, tx, batch)
				case Delete:
					err = w.deleteBatch(ctx, logger, tx, batch)
				case Update:
					err = w.updateBatch(ctx, logger, tx, batch)
				default:
					logger.Panicf("Unknown batch type %s", batch.Type)
					return nil
				}
			}
			return err
		})

		// These should not be retried
		if isSchemaError(err) {
			return backoff.Permanent(err)
		}
		if isConstraintViolation(err) {
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
	table := batch.Table
	logger.Debugf("deleting %d rows", len(rows))

	// We don't use batch statements for deletes, instead we prepare a single statement and do all the updates
	// in the same transaction

	comparison, _ := expandRowConstructorComparison(table.KeyColumns, "=",
		make([]interface{}, len(table.KeyColumns)))
	stmt := fmt.Sprintf("DELETE FROM `%s` WHERE %s",
		table.Name, comparison)
	prepared, err := tx.PrepareContext(ctx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not prepare: %s", stmt)
	}
	defer prepared.Close()

	for _, row := range rows {
		args := row.KeyValues()

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
		stmt := fmt.Sprintf("INSERT IGNORE INTO %s (%s) VALUES %s",
			table.Name, table.ColumnList, strings.Join(valueStrings, ","))
		result, err := tx.ExecContext(ctx, stmt, valueArgs...)
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
		for _, keyColumn := range table.KeyColumns {
			if column == keyColumn {
				continue
			}
		}
		c := fmt.Sprintf("`%s` = ?", column)
		columnValues = append(columnValues, c)
	}

	// We don't use batch statements for updates, instead we prepare a single statement and do all the updates
	// in the same transaction

	comparison, _ := expandRowConstructorComparison(table.KeyColumns, "=",
		make([]interface{}, len(table.KeyColumns)))
	stmt := fmt.Sprintf("UPDATE `%s` SET %s WHERE %s",
		table.Name, strings.Join(columnValues, ","), comparison)
	prepared, err := tx.PrepareContext(ctx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not prepare: %s", stmt)
	}
	defer prepared.Close()

	for _, row := range rows {
		args := make([]interface{}, 0, len(columns))
		for i, column := range columns {
			for _, keyColumn := range table.KeyColumns {
				if column == keyColumn {
					continue
				}
			}
			args = append(args, row.Data[i])
		}
		args = row.AppendKeyValues(args)

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
	table  *Table

	db    *sql.DB
	retry RetryOptions

	writerParallelism *semaphore.Weighted
}

func NewWriter(config WriterConfig, table *Table, writer *sql.DB, limiter core.Limiter) *Writer {
	return &Writer{
		config: config,
		table:  table,
		db:     writer,
		retry: RetryOptions{
			Limiter:       limiter,
			AcquireMetric: writeLimiterDelay,
			MaxRetries:    config.WriteRetries,
			Timeout:       config.WriteTimeout,
		},
		writerParallelism: semaphore.NewWeighted(config.WriterParallelism),
	}
}

// Write forks off two go routines to batch and write the batches from the diffs channel
func (w *Writer) Write(ctx context.Context, g *errgroup.Group, diffs chan Diff) {
	// Batch up the diffs
	batches := make(chan Batch)
	g.Go(func() error {
		err := BatchTableWrites(ctx, diffs, batches)
		close(batches)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	// Write every batch
	g.Go(func() error {
		g, ctx := errgroup.WithContext(ctx)
		inserts := 0
		deletes := 0
		updates := 0

		for batch := range batches {
			switch batch.Type {
			case Insert:
				inserts += len(batch.Rows)
			case Delete:
				deletes += len(batch.Rows)
			case Update:
				updates += len(batch.Rows)
			}
			err := w.scheduleWriteBatch(ctx, g, batch)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		err := g.Wait()
		if err != nil {
			return errors.WithStack(err)
		}
		logger := log.WithContext(ctx).WithField("task", "writer").WithField("table", w.table.Name)
		logger.Infof("writes done: %s (inserts=%d deletes=%d updates=%d)",
			w.table.Name, inserts, deletes, updates)
		return nil
	})
}
