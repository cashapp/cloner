package clone

import (
	"context"
	"database/sql"
	"fmt"
	_ "net/http/pprof"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/mightyguava/autotx"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type NewClone struct {
	WriterConfig

	ChunkParallelism int  `help:"Number of chunks to per table to repair concurrently" default:"1000"`
	CopySchema       bool `help:"Whether to copy the schema or not, will not do incremental schema updates" default:"false"`
	RepairAttempts   int  `help:"How many attempts are made to repair the same chunk, clone will fail if not successful" default:"1"`
}

// Run finds any differences between source and target
func (cmd *NewClone) Run() error {
	var err error

	start := time.Now()

	err = cmd.ReaderConfig.LoadConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	logrus.Infof("using config: %v", cmd)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err = cmd.run(ctx)

	elapsed := time.Since(start)
	logger := logrus.WithField("duration", elapsed)
	if err != nil {
		if stackErr, ok := err.(stackTracer); ok {
			logger = logger.WithField("stacktrace", stackErr.StackTrace())
		}
		logger.WithError(err).Errorf("error: %+v", err)
	} else {
		logger.Infof("full clone success")
	}

	return errors.WithStack(err)
}

func (cmd *NewClone) run(ctx context.Context) error {
	if cmd.TableParallelism == 0 {
		return errors.Errorf("need more parallelism")
	}

	// Load tables
	tables, err := LoadTables(ctx, cmd.ReaderConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	sourceReader, err := cmd.Source.ReaderDB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer sourceReader.Close()
	// Refresh connections regularly so they don't go stale
	sourceReader.SetConnMaxLifetime(time.Minute)
	sourceReader.SetMaxOpenConns(cmd.ReaderCount)
	sourceReaderCollector := sqlstats.NewStatsCollector("source_reader", sourceReader)
	prometheus.MustRegister(sourceReaderCollector)
	defer prometheus.Unregister(sourceReaderCollector)

	// Target reader
	// We can use a connection pool of unsynced connections for the target because the assumption is there are no
	// other writers to the target during the clone
	// TODO we only have to open the target DB if NoDiff is set to false
	targetReader, err := cmd.Target.ReaderDB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer targetReader.Close()
	// Refresh connections regularly so they don't go stale
	targetReader.SetConnMaxLifetime(time.Minute)
	targetReader.SetMaxOpenConns(cmd.ReaderCount)
	targetReaderCollector := sqlstats.NewStatsCollector("target_reader", targetReader)
	prometheus.MustRegister(targetReaderCollector)
	defer prometheus.Unregister(targetReaderCollector)

	writer, err := cmd.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer writer.Close()
	// Refresh connections regularly so they don't go stale
	writer.SetConnMaxLifetime(time.Minute)
	writer.SetMaxOpenConns(cmd.WriterCount)
	writerCollector := sqlstats.NewStatsCollector("target_writer", writer)
	prometheus.MustRegister(writerCollector)
	defer prometheus.Unregister(writerCollector)

	var tablesToDo []string
	for _, t := range tables {
		tablesToDo = append(tablesToDo, t.Name)
	}
	logrus.Infof("starting clone")

	var estimatedRows int64
	tablesTotalMetric.Set(float64(len(tables)))
	for _, table := range tables {
		estimatedRows += table.EstimatedRows
		rowCountMetric.WithLabelValues(table.Name).Set(float64(table.EstimatedRows))
	}

	if cmd.UseConcurrencyLimits {
		return errors.Errorf("concurrency limits no longer supported")
	}

	writeLogger := NewThroughputLogger("write", cmd.ThroughputLoggingFrequency, 0)
	readLogger := NewThroughputLogger("read", cmd.ThroughputLoggingFrequency, uint64(estimatedRows))

	if cmd.CopySchema {
		err := cmd.copySchema(ctx, tables, sourceReader, writer)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(cmd.TableParallelism)

	tablesDoneCh := make(chan string)
	g.Go(func() error {
		tablesLeft := tablesToDo
		logger := logrus.WithField("task", "clone")
		for {
			if len(tablesLeft) == 0 {
				return nil
			}
			select {
			case <-ctx.Done():
				return nil
			case table := <-tablesDoneCh:
				tablesLeft = removeElement(tablesLeft, table)
				logger.Infof("table done: %v tables left: %v", table, strings.Join(tablesLeft, ","))
			}
		}
	})

	for _, t := range tables {
		table := t
		if err != nil {
			return errors.WithStack(err)
		}
		g.Go(func() error {
			repairer := NewTableRepairer(cmd, table, sourceReader, writer, readLogger, writeLogger)
			err := repairer.Run(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			tablesDoneCh <- table.Name

			return nil
		})
	}

	err = g.Wait()
	logrus.WithField("task", "clone").Infof("full clone done")
	return err
}

type TableRepairer struct {
	config      *NewClone
	table       *Table
	source      *sql.DB
	target      *sql.DB
	readLogger  *ThroughputLogger
	writeLogger *ThroughputLogger
	readRetry   RetryOptions
}

func (r *TableRepairer) Run(ctx context.Context) error {
	chunks := make(chan Chunk)
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(1 + r.config.ChunkParallelism)
	g.Go(func() error {
		defer func() { close(chunks) }()
		err := generateTableChunksAsync(ctx, r.table, r.source, chunks, r.readRetry)
		return errors.WithStack(err)
	})
	for c := range chunks {
		chunk := c
		g.Go(func() error {
			logrus.Debugf("repairing %v", chunk.String())
			if r.config.NoDiff {
				err := r.write(ctx, chunk)
				return errors.WithStack(err)
			} else {
				err := r.repair(ctx, chunk)
				return errors.WithStack(err)
			}
		})
	}
	err := g.Wait()
	return errors.WithStack(err)
}

func (r *TableRepairer) diffChunk(ctx context.Context, chunk Chunk) ([]Diff, error) {
	var sizeBytes uint64
	timer := prometheus.NewTimer(diffDuration.WithLabelValues(chunk.Table.Name))
	defer func() {
		timer.ObserveDuration()
		chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()
		r.readLogger.Record(chunk.Table.Name, chunk.Size, sizeBytes)
		readsBytes.WithLabelValues(chunk.Table.Name).Add(float64(sizeBytes))
		rowsProcessed.WithLabelValues(chunk.Table.Name).Add(float64(chunk.Size))
	}()

	if r.config.UseCRC32Checksum {
		// start off by running a fast checksum query
		sourceChecksum, err := checksumChunk(ctx, r.readRetry, "source", r.source, chunk)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		targetChecksum, err := checksumChunk(ctx, r.readRetry, "target", r.target, chunk)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if sourceChecksum == targetChecksum {
			// Checksums match, no need to do any further diffing
			return nil, nil
		}
	}

	sourceStream, sizeBytes, err := bufferChunk(ctx, r.readRetry, r.source, "source", chunk)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Sort the snapshot using genericCompare which diff depends on
	sourceStream.sort()
	targetStream, _, err := bufferChunk(ctx, r.readRetry, r.target, "target", chunk)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	diffs, err := StreamDiff(ctx, chunk.Table, sourceStream, targetStream)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Sort the snapshot using genericCompare which diff depends on
	targetStream.sort()

	return diffs, nil
}

func (r *TableRepairer) insertBatch(ctx context.Context, logger *logrus.Entry, tx *sql.Tx, batch Batch) error {
	logger = logger.WithField("op", "insert")
	logger.Debugf("inserting %d rows", len(batch.Rows))

	table := batch.Table
	columns := table.Columns
	questionMarks := make([]string, 0, len(columns))
	for range columns {
		questionMarks = append(questionMarks, "?")
	}
	values := fmt.Sprintf("(%s)", strings.Join(questionMarks, ","))

	statementBatches := batches(batch.Rows, r.config.WriteBatchStatementSize)
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

func (r *TableRepairer) updateBatch(ctx context.Context, logger *logrus.Entry, tx *sql.Tx, batch Batch) error {
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

func (r *TableRepairer) deleteBatch(ctx context.Context, logger *logrus.Entry, tx *sql.Tx, batch Batch) error {
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

func (r *TableRepairer) replaceBatch(ctx context.Context, logger *logrus.Entry, tx *sql.Tx, batch Batch) error {
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

	statementBatches := batches(batch.Rows, r.config.WriteBatchStatementSize)
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

func (r *TableRepairer) writeBatch(ctx context.Context, batch Batch) (err error) {
	logger := logrus.WithField("task", "writer").WithField("table", batch.Table.Name)

	err = autotx.TransactWithRetryAndOptions(ctx, r.target, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, autotx.RetryOptions{
		MaxRetries: int(r.config.WriteRetries),
		IsRetryable: func(err error) bool {
			return !isSchemaError(err) && !isConstraintViolation(err)
		},
	}, func(tx *sql.Tx) error {
		timer := prometheus.NewTimer(writeDuration.WithLabelValues(batch.Table.Name, string(batch.Type)))
		defer timer.ObserveDuration()
		defer func() {
			if err == nil {
				sizeBytes := batch.SizeBytes()
				r.writeLogger.Record(batch.Table.Name, len(batch.Rows), sizeBytes)
				writesBytes.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(sizeBytes))
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

		if r.config.NoDiff {
			err = r.replaceBatch(ctx, logger, tx, batch)
		} else {
			switch batch.Type {
			case Insert:
				err = r.insertBatch(ctx, logger, tx, batch)
			case Delete:
				err = r.deleteBatch(ctx, logger, tx, batch)
			case Update:
				err = r.updateBatch(ctx, logger, tx, batch)
			default:
				logger.Panicf("Unknown batch type %s", batch.Type)
				return nil
			}
		}
		return err
	})

	if err != nil {
		if errors.Is(err, context.Canceled) {
			return errors.WithStack(err)
		}

		if isConstraintViolation(err) {
			constraintViolationErrors.WithLabelValues(batch.Table.Name, batch.Type.String()).Inc()
		}
		if isSchemaError(err) {
			schemaErrors.WithLabelValues(batch.Table.Name, batch.Type.String()).Inc()
		}

		// If we fail to write we'll split the batch
		// * It could be because of a conflict violation in which case we want to write all the non-conflicting rows
		// * It could be because some rows in the batch are too big in which case we want to decrease the batch size
		// In either case splitting the batch is better
		if len(batch.Rows) > 1 {
			batch1, batch2 := splitBatch(batch)
			err = r.writeBatch(ctx, batch1)
			if err != nil {
				return errors.WithStack(err)
			}
			err = r.writeBatch(ctx, batch2)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		}

		return nil
	}

	return nil
}

func (r *TableRepairer) attemptRepair(ctx context.Context, chunk Chunk) (hadDiffs bool, err error) {
	diffs, err := r.diffChunk(ctx, chunk)
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	if len(diffs) > 0 {
		hadDiffs = true
		chunksWithDiffs.WithLabelValues(r.table.Name).Inc()
	} else {
		hadDiffs = false
		return
	}

	// Batch up the diffs
	batches, err := BatchTableWritesSync(diffs)
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	// Write every batch
	for _, batch := range batches {
		err = r.writeBatch(ctx, batch)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
	}

	return
}

func (r *TableRepairer) repair(ctx context.Context, chunk Chunk) error {
	b := backoff.NewExponentialBackOff()
	i := 0
	for {
		diffs, err := r.diffChunk(ctx, chunk)
		if err != nil {
			return errors.WithStack(err)
		}

		if len(diffs) == 0 {
			return nil
		}

		if i == 0 {
			// Only count the chunk once
			chunksWithDiffs.WithLabelValues(r.table.Name).Inc()
		}

		if i > r.config.RepairAttempts {
			return errors.Errorf("failed to repair chunk %v after %d attempts, "+
				"either increase --repair-attempts or decrease chunk size or both", chunk.String(), r.config.RepairAttempts)
		}

		err = r.writeDiffs(ctx, diffs)
		if err != nil {
			return errors.WithStack(err)
		}

		if i > 0 {
			// Don't back off the first attempt
			time.Sleep(b.NextBackOff())
		}

		i++
	}
}

func (r *TableRepairer) readChunk(ctx context.Context, chunk Chunk) ([]Diff, error) {
	var sizeBytes uint64
	timer := prometheus.NewTimer(diffDuration.WithLabelValues(chunk.Table.Name))
	defer func() {
		timer.ObserveDuration()
		r.readLogger.Record(chunk.Table.Name, chunk.Size, sizeBytes)
		chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()
		rowsProcessed.WithLabelValues(chunk.Table.Name).Add(float64(chunk.Size))
	}()

	sourceStream, sizeBytes, err := bufferChunk(ctx, r.readRetry, r.source, "source", chunk)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	diffs := make([]Diff, 0, chunk.Size)
	for {
		row, err := sourceStream.Next()
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if row == nil {
			break
		}
		readsProcessed.WithLabelValues(row.Table.Name, "source").Inc()
		diffs = append(diffs, Diff{Insert, row, nil})
	}

	return diffs, nil
}

func (r *TableRepairer) write(ctx context.Context, chunk Chunk) error {
	diffs, err := r.readChunk(ctx, chunk)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(diffs) == 0 {
		return nil
	}
	err = r.writeDiffs(ctx, diffs)
	return errors.WithStack(err)
}

func (r *TableRepairer) writeDiffs(ctx context.Context, diffs []Diff) error {
	// Batch up the diffs
	batches, err := BatchTableWritesSync(diffs)
	if err != nil {
		return errors.WithStack(err)
	}

	// Write every batch
	for _, batch := range batches {
		err = r.writeBatch(ctx, batch)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func NewTableRepairer(config *NewClone, table *Table, reader *sql.DB, writer *sql.DB, readLogger *ThroughputLogger, writeLogger *ThroughputLogger) *TableRepairer {
	return &TableRepairer{
		config:      config,
		table:       table,
		source:      reader,
		target:      writer,
		readLogger:  readLogger,
		writeLogger: writeLogger,
		readRetry: RetryOptions{
			MaxRetries: config.ReadRetries,
			Timeout:    config.ReadTimeout,
		},
	}
}

func (cmd *NewClone) copySchema(ctx context.Context, tables []*Table, source *sql.DB, target *sql.DB) error {
	for _, table := range tables {
		err := cmd.copyTableSchema(ctx, table, source, target)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (cmd *NewClone) copyTableSchema(ctx context.Context, table *Table, source *sql.DB, target *sql.DB) error {
	rows, err := source.QueryContext(ctx, fmt.Sprintf("SHOW CREATE TABLE %v", table.Name))
	if err != nil {
		return errors.WithStack(err)
	}
	defer rows.Close()
	var name string
	var ddl string
	if !rows.Next() {
		return errors.Errorf("could not find schema for table %v", table.Name)
	}
	err = rows.Scan(&name, &ddl)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = target.ExecContext(ctx, ddl)
	if err != nil {
		me := mysqlError(err)
		if me != nil {
			if me.Number == 1050 {
				// Table already exists
				return nil
			}
		}
		return errors.WithStack(err)
	}
	return nil
}

func removeElement[T comparable](slice []T, element T) []T {
	return removeElementByIndex(slice, findIndex(slice, func(t T) bool {
		return element == t
	}))
}

func removeElementByIndex[T any](slice []T, index int) []T {
	return append(slice[:index], slice[index+1:]...)
}

func findIndex[T any](slice []T, matchFunc func(T) bool) int {
	for index, element := range slice {
		if matchFunc(element) {
			return index
		}
	}

	return -1 // not found
}
