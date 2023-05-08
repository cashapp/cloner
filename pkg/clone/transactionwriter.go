package clone

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	_ "net/http/pprof"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"
	"unsafe"

	"github.com/cenkalti/backoff/v4"
	"github.com/dlmiddlecote/sqlstats"
	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/mightyguava/autotx"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// TransactionWriter receives transactions and requests to snapshot and writes transactions and strongly consistent chunk snapshots
type TransactionWriter struct {
	config          Replicate
	target          *sql.DB
	targetCollector *sqlstats.StatsCollector

	targetRetry     RetryOptions
	replicateLogger *ThroughputLogger
	repairLogger    *ThroughputLogger
}

func NewTransactionWriter(config Replicate) (*TransactionWriter, error) {
	replicateLogger := NewThroughputLogger("replication", config.ThroughputLoggingFrequency, 0)
	snapshotLogger := NewThroughputLogger("snapshot write", config.ThroughputLoggingFrequency, 0)

	var err error
	w := TransactionWriter{
		config:          config,
		replicateLogger: replicateLogger,
		repairLogger:    snapshotLogger,
		targetRetry: RetryOptions{
			Limiter:       nil, // will we ever use concurrency limiter again? probably not?
			AcquireMetric: readLimiterDelay.WithLabelValues("target"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
	}
	target, err := w.config.Target.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	target.SetConnMaxLifetime(time.Minute)
	w.target = target
	w.targetCollector = sqlstats.NewStatsCollector("target", target)

	return &w, nil
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
	prometheus.MustRegister(w.targetCollector)
	defer prometheus.Unregister(w.targetCollector)

	if w.config.ReplicationParallelism == 1 {
		return errors.WithStack(w.runSequential(ctx, b, transactions))
	} else {
		return errors.WithStack(w.runParallel(ctx, b, transactions))
	}
}

// runSequential implements sequential replication
func (w *TransactionWriter) runSequential(ctx context.Context, b backoff.BackOff, transactions chan Transaction) error {
	for {
		var transaction Transaction
		select {
		case transaction = <-transactions:
		case <-ctx.Done():
			return ctx.Err()
		}

		err := w.transact(ctx, func(tx *sql.Tx) error {
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

type pkSet struct {
	table      *Table
	mysqlTable *mysqlschema.Table
	m          map[uint64][][]interface{}
}

// Intersects checks if any PKs in the set belong to the chunk
func (s *pkSet) Intersects(chunk Chunk) bool {
	for _, pks := range s.m {
		for _, pk := range pks {
			if chunk.ContainsKeys(pk) {
				return true
			}
		}
	}
	return false
}

func (s *pkSet) ContainsRow(row []interface{}) bool {
	pkValues, err := s.keysOfRow(row)
	if err != nil {
		panic(err)
	}
	return s.ContainsKeys(pkValues)
}

func (s *pkSet) keysOfRow(row []interface{}) ([]interface{}, error) {
	if s.table != nil {
		return s.table.KeysOfRow(row), nil
	}
	return s.mysqlTable.GetPKValues(row)
}

func (s *pkSet) ContainsKeys(keys []interface{}) bool {
	hash, err := hashstructure.Hash(keys, hashstructure.FormatV2, nil)
	if err != nil {
		panic(err)
	}
	vals, exists := s.m[hash]
	if !exists {
		return false
	}
	for _, val := range vals {
		if reflect.DeepEqual(keys, val) {
			return true
		}
	}
	return false
}

func (s *pkSet) AddRow(row []interface{}) {
	keys, err := s.keysOfRow(row)
	if err != nil {
		panic(err)
	}
	s.AddKeys(keys)
}

func (s *pkSet) AddKeys(keys []interface{}) {
	if s.ContainsKeys(keys) {
		// We already got it
		return
	}
	hash, err := hashstructure.Hash(keys, hashstructure.FormatV2, nil)
	if err != nil {
		panic(err)
	}
	s.m[hash] = append(s.m[hash], keys)
}

func (s *pkSet) String() string {
	var vals []string
	for _, pks := range s.m {
		for _, pk := range pks {
			for _, val := range pk {
				vals = append(vals, fmt.Sprintf("%v", val))
			}
		}
	}
	return strings.Join(vals, " ")
}

type orderedTransaction struct {
	ordinal     int64
	transaction Transaction
}

type transactionSequence struct {
	writer *TransactionWriter
	// chunks is the list of chunks this sequence repair
	chunks []Chunk
	// primaryKeys caches the primary key sets of this sequence, keyed by table name
	primaryKeys  map[string]*pkSet
	transactions []orderedTransaction
}

func (s *transactionSequence) Print(ctx context.Context) {
	fmt.Printf("%v\n", s.PKSetString())
	for _, transaction := range s.transactions {
		if len(transaction.transaction.Mutations) == 0 {
			continue
		}
		fmt.Printf("---\n")
		for _, mutation := range transaction.transaction.Mutations {
			if mutation.Type == Repair {
				fmt.Printf("repair %v-%v\n", mutation.Chunk.Start, mutation.Chunk.End)
			} else {
				writer := &printingWriter{target: s.writer.target}
				_, _, _ = mutation.Write(ctx, writer)
			}
		}
	}
}

func (s *transactionSequence) IsCausal(transaction Transaction) bool {
	for _, chunk := range s.chunks {
		for _, mutation := range transaction.Mutations {
			if mutation.Type == Repair {
				// Chunks never overlap so this mutation is certainly not causal
				continue
			}
			if mutation.Table.Name != chunk.Table.Name {
				// Not the same table so certainly not causal
				continue
			}

			// Same table and not a Repair so go through all the PKs to check if they are inside this chunk
			for _, row := range mutation.Before {
				if chunk.ContainsRow(row) {
					return true
				}
			}
			for _, row := range mutation.Rows {
				if chunk.ContainsRow(row) {
					// Yep, causal
					return true
				}
			}
		}
	}
	for _, mutation := range transaction.Mutations {
		if mutation.Type == Repair {
			for tableName, pks := range s.primaryKeys {
				chunk := mutation.Chunk
				if chunk.Table.Name != tableName {
					continue
				}
				if pks.Intersects(chunk) {
					return true
				}
			}
		} else {
			pks, exists := s.primaryKeys[mutation.Table.Name]
			if !exists {
				// This sequence doesn't touch this table (yet?) so not causal with these mutations for sure
				continue
			}
			for _, row := range mutation.Before {
				if pks.ContainsRow(row) {
					return true
				}
			}
			for _, row := range mutation.Rows {
				if pks.ContainsRow(row) {
					return true
				}
			}
		}
	}
	return false
}

func (s *transactionSequence) Append(transaction orderedTransaction) {
	s.transactions = append(s.transactions, transaction)

	// update the cache of the primary key set
	for _, mutation := range transaction.transaction.Mutations {
		if mutation.Type == Repair {
			s.chunks = append(s.chunks, mutation.Chunk)
		} else {
			pks, exists := s.primaryKeys[mutation.Table.Name]
			if !exists {
				pks = &pkSet{
					table:      mutation.Table,
					mysqlTable: mutation.Table.MysqlTable,
					m:          make(map[uint64][][]interface{}),
				}
				s.primaryKeys[mutation.Table.Name] = pks
			}
			for _, row := range mutation.Before {
				pks.AddRow(row)
			}
			for _, row := range mutation.Rows {
				pks.AddRow(row)
			}
		}
	}
}

func (s *transactionSequence) Run(ctx context.Context) error {
	for _, transaction := range s.transactions {
		if len(transaction.transaction.Mutations) == 0 {
			continue
		}
		err := s.writer.transact(ctx, func(tx *sql.Tx) error {
			for _, mutation := range transaction.transaction.Mutations {
				err := s.writer.handleMutation(ctx, tx, mutation)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			return nil
		})
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

func (s *transactionSequence) PKSetString() string {
	var result []string
	for _, c := range s.chunks {
		result = append(result, fmt.Sprintf("%s: [%d - %d]", c.Table.Name, c.Start, c.End))
	}
	for table, pks := range s.primaryKeys {
		result = append(result, fmt.Sprintf("%s: [%v]", table, pks))
	}
	return strings.Join(result, " ")
}

func PKSetString(t Transaction) string {
	seq := transactionSequence{primaryKeys: make(map[string]*pkSet)}
	seq.Append(orderedTransaction{transaction: t})
	return seq.PKSetString()
}

type transactionSet struct {
	writer *TransactionWriter

	sequences     []*transactionSequence
	ordinal       int64
	finalPosition Position
	g             *errgroup.Group
	timer         *prometheus.Timer
}

func (s *transactionSet) Append(t Transaction) {
	transaction := orderedTransaction{
		ordinal:     s.ordinal,
		transaction: t,
	}
	s.ordinal++
	s.finalPosition = transaction.transaction.FinalPosition
	var sequences []*transactionSequence
	for _, sequence := range s.sequences {
		if sequence.IsCausal(transaction.transaction) {
			sequences = append(sequences, sequence)
		}
	}
	var sequence *transactionSequence
	if len(sequences) == 0 {
		// Non-causal with any of the existing sequences, so we create a new sequence for this transaction
		sequence = &transactionSequence{writer: s.writer, primaryKeys: make(map[string]*pkSet)}
		s.sequences = append(s.sequences, sequence)
	} else if len(sequences) == 1 {
		sequence = sequences[0]
	} else {
		// This transaction spans multiple sequences which conjoins them and makes them all causal
		// we have to merge them all together into a single sequence
		// To merge sequences we grab all the transactions from all sequences
		var transactions []orderedTransaction
		for _, seq := range sequences {
			transactions = append(transactions, seq.transactions...)
		}
		// sort them by time order
		sort.Slice(transactions, func(i, j int) bool {
			return transactions[i].ordinal < transactions[j].ordinal
		})
		// add them to a fresh sequence
		sequence = &transactionSequence{writer: s.writer, primaryKeys: make(map[string]*pkSet)}
		for _, t := range transactions {
			sequence.Append(t)
		}
		// then remove all the merged sequences
		for _, seq := range sequences {
			for i, sq := range s.sequences {
				if sq == seq {
					s.sequences = append(s.sequences[:i], s.sequences[i+1:]...)
					break
				}
			}
		}
		// and add the new union sequence
		s.sequences = append(s.sequences, sequence)
	}

	sequence.Append(transaction)
}

func (s *transactionSet) Wait() error {
	defer s.timer.ObserveDuration()
	return errors.WithStack(s.g.Wait())
}

func (s *transactionSet) Start(parent context.Context) {
	if s.g != nil {
		panic("can't start twice")
	}

	s.timer = prometheus.NewTimer(replicationParallelismApplyDuration.WithLabelValues(s.writer.config.TaskName))
	replicationParallelism.WithLabelValues(s.writer.config.TaskName).Set(float64(len(s.sequences)))
	replicationParallelismBatchSize.WithLabelValues(s.writer.config.TaskName).Set(float64(s.ordinal))
	logrus.WithContext(parent).WithField("task", "replicate").
		Debugf("starting a batch of %d transactions run in %d parallel sequences with actual parallelism of %d",
			s.ordinal, len(s.sequences), s.writer.config.ReplicationParallelism)

	g, ctx := errgroup.WithContext(parent)
	g.SetLimit(s.writer.config.ReplicationParallelism)
	s.g = g
	for _, seq := range s.sequences {
		sequence := seq
		s.g.Go(func() error {
			err := sequence.Run(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			return nil
		})
	}
}

type printingWriter struct {
	target DBWriter
}

func (l *printingWriter) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	fmt.Printf("%v %v\n", query, args)
	return l.target.QueryContext(ctx, query, args...)
}

func (l *printingWriter) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	fmt.Printf("%v %v\n", query, args)
	return driver.RowsAffected(0), nil
}

func (s *transactionSet) Print(ctx context.Context) {
	for _, seq := range s.sequences {
		if len(seq.transactions) == 0 {
			continue
		}
		fmt.Println("serially: --------------")
		seq.Print(ctx)
	}
}

// runParallel implements parallelized replication
func (w *TransactionWriter) runParallel(ctx context.Context, b backoff.BackOff, transactions chan Transaction) error {
	var currentlyExecutingTransactionSet *transactionSet
	for {
		nextTransactionSet, err := w.fillTransactionSet(ctx, transactions)
		if err != nil {
			return errors.WithStack(err)
		}

		// Wait for the currently executing transaction set to complete running
		if currentlyExecutingTransactionSet != nil {
			err := currentlyExecutingTransactionSet.Wait()
			if err != nil {
				return errors.WithStack(err)
			}
			err = autotx.TransactWithOptions(ctx, w.target, &sql.TxOptions{Isolation: sql.LevelReadCommitted}, func(tx *sql.Tx) error {
				err := w.writeCheckpoint(ctx, tx, currentlyExecutingTransactionSet.finalPosition)
				return errors.WithStack(err)
			})
			if err != nil {
				return errors.WithStack(err)
			}

			// We've committed a transaction set, we can reset the backoff
			b.Reset()
		}
		currentlyExecutingTransactionSet = nextTransactionSet
		currentlyExecutingTransactionSet.Start(ctx)
	}
}

func (w *TransactionWriter) fillTransactionSet(ctx context.Context, transactions chan Transaction) (*transactionSet, error) {
	size := 0
	nextTransactionSet := &transactionSet{writer: w}
	transactionSetTimeout := time.After(w.config.ParallelTransactionBatchTimeout)

	// Fill the next transaction set before the transaction set timeout expires
	for {
		select {
		case transaction := <-transactions:
			size++
			nextTransactionSet.Append(transaction)
			if size >= w.config.ParallelTransactionBatchMaxSize {
				return nextTransactionSet, nil
			}
		case <-transactionSetTimeout:
			if size == 0 {
				// We have no transactions, so we might as well wait a bit longer
				transactionSetTimeout = time.After(w.config.ParallelTransactionBatchTimeout)
				continue
			}
			return nextTransactionSet, nil
		case <-ctx.Done():
			return nextTransactionSet, ctx.Err()
		}
	}
}

func (w *TransactionWriter) handleMutation(ctx context.Context, tx *sql.Tx, m Mutation) error {
	if m.Table.Name == w.config.WatermarkTable {
		// We don't send writes to the watermark table to the target
		return nil
	}
	rowCount, sizeBytes, err := m.Write(ctx, tx)
	if err != nil {
		return errors.WithStack(err)
	}

	switch m.Type {
	case Repair:
		w.repairLogger.Record(m.Table.Name, rowCount, sizeBytes)
	case Delete, Insert, Update:
		w.replicateLogger.Record(m.Table.Name, rowCount, sizeBytes)
	default:
		panic(fmt.Sprintf("unknown mutation type: %d", m.Type))
	}

	return nil
}

func (m *Mutation) Write(ctx context.Context, tx DBWriter) (rowCount int, sizeBytes uint64, err error) {
	switch m.Type {
	case Repair:
		rowCount, sizeBytes, err = m.repair(ctx, tx)
	case Delete:
		err = m.delete(ctx, tx)
		rowCount = len(m.Rows)
		sizeBytes = m.SizeBytes()
	case Insert, Update:
		err = m.replace(ctx, tx)
		rowCount = len(m.Rows)
		sizeBytes = m.SizeBytes()
	default:
		panic(fmt.Sprintf("unknown mutation type: %d", m.Type))
	}

	return
}

func (m *Mutation) replace(ctx context.Context, tx DBWriter) error {
	var err error
	tableSchema := m.Table.MysqlTable
	tableName := tableSchema.Name
	writeType := m.Type.String()
	timer := prometheus.NewTimer(writeDuration.WithLabelValues(tableName, writeType))
	defer timer.ObserveDuration()
	defer func() {
		if err == nil {
			writesSucceeded.WithLabelValues(tableName, writeType).Add(float64(len(m.Rows)))
		} else {
			mySQLError := mysqlError(err)
			var errorCode uint16
			if mySQLError != nil {
				errorCode = mySQLError.Number
			}
			writesFailed.WithLabelValues(tableName, writeType, strconv.Itoa(int(errorCode))).
				Add(float64(len(m.Rows)))
		}
	}()
	var questionMarks strings.Builder
	var columnListBuilder strings.Builder
	for i, column := range tableSchema.Columns {
		if m.Table.IgnoredColumnsBitmap[i] {
			continue
		}
		if i != 0 {
			columnListBuilder.WriteString(",")
			questionMarks.WriteString(",")
		}
		questionMarks.WriteString("?")
		columnListBuilder.WriteString("`")
		columnListBuilder.WriteString(column.Name)
		columnListBuilder.WriteString("`")
	}
	values := fmt.Sprintf("(%s)", questionMarks.String())
	columnList := columnListBuilder.String()

	valueStrings := make([]string, 0, len(m.Rows))
	valueArgs := make([]interface{}, 0, len(m.Rows)*len(tableSchema.Columns))
	for _, row := range m.Rows {
		valueStrings = append(valueStrings, values)
		for i, val := range row {
			if !m.Table.IgnoredColumnsBitmap[i] {
				valueArgs = append(valueArgs, val)
			}
		}
	}
	// TODO build the entire statement with a strings.Builder like in delete below. For speed.
	stmt := fmt.Sprintf("REPLACE INTO %s (%s) VALUES %s",
		tableSchema.Name, columnList, strings.Join(valueStrings, ","))
	_, err = tx.ExecContext(ctx, stmt, valueArgs...)
	if err != nil {
		return errors.Wrapf(err, "could not execute: %s", stmt)
	}

	return nil
}

func (m *Mutation) SizeBytes() (size uint64) {
	size = 0
	for _, row := range m.Rows {
		size += uint64(unsafe.Sizeof(row))
	}
	return
}

func (m *Mutation) delete(ctx context.Context, tx DBWriter) (err error) {
	tableSchema := m.Table.MysqlTable
	tableName := tableSchema.Name
	writeType := m.Type.String()
	timer := prometheus.NewTimer(writeDuration.WithLabelValues(tableName, writeType))
	defer timer.ObserveDuration()
	defer func() {
		if err == nil {
			writesSucceeded.WithLabelValues(tableName, writeType).Add(float64(len(m.Rows)))
		} else {
			mySQLError := mysqlError(err)
			var errorCode uint16
			if mySQLError != nil {
				errorCode = mySQLError.Number
			}
			writesFailed.WithLabelValues(tableName, writeType, strconv.Itoa(int(errorCode))).
				Add(float64(len(m.Rows)))
		}
	}()
	var stmt strings.Builder
	args := make([]interface{}, 0, len(m.Rows))
	stmt.WriteString("DELETE FROM `")
	stmt.WriteString(m.Table.Name)
	stmt.WriteString("` WHERE ")
	for rowIdx, row := range m.Rows {
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

		if rowIdx != len(m.Rows)-1 {
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
			task        VARCHAR(255) NOT NULL,
			file        VARCHAR(255) NOT NULL,
			position    BIGINT(20)   NOT NULL,
			source_gtid TEXT,
			target_gtid TEXT,
			timestamp   TIMESTAMP    NOT NULL,
			PRIMARY KEY (task)
		)
		`, "`"+w.config.CheckpointTable+"`")
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
	targetGTIDColumn := ""
	targetGTIDValue := ""
	if w.config.SaveGTIDExecuted {
		targetGTIDColumn = ", target_gtid"
		targetGTIDValue = ", @@GLOBAL.gtid_executed"
	}
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf("REPLACE INTO %s (task, file, position, source_gtid, timestamp%s) VALUES (?, ?, ?, ?, ?%s)",
			w.config.CheckpointTable, targetGTIDColumn, targetGTIDValue),
		w.config.TaskName, position.File, position.Position, gsetString, time.Now().UTC())
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func (w *TransactionWriter) transact(ctx context.Context, f func(tx *sql.Tx) error) error {
	return errors.WithStack(autotx.TransactWithRetryAndOptions(ctx,
		w.target,
		&sql.TxOptions{Isolation: sql.LevelReadCommitted},
		autotx.RetryOptions{
			MaxRetries: int(w.config.WriteRetries),
			IsRetryable: func(err error) bool {
				return !isSchemaError(err)
			},
		}, f))
}

// repair synchronously diffs and writes the chunk to the target (diff and write)
// the writes are made synchronously in the replication stream to maintain strong consistency
func (m *Mutation) repair(ctx context.Context, tx DBWriter) (rowCount int, sizeBytes uint64, err error) {
	targetStream, _, err := readChunk(ctx, tx, "target", m.Chunk)
	if err != nil {
		return rowCount, sizeBytes, errors.WithStack(err)
	}

	// Diff the streams
	// TODO StreamDiff should return []Mutation here instead
	diffs, err := StreamDiff(ctx, m.Table, stream(m.Table, m.Rows), targetStream)
	if err != nil {
		return rowCount, sizeBytes, errors.WithStack(err)
	}

	if len(diffs) > 0 {
		chunksWithDiffs.WithLabelValues(m.Table.Name).Inc()
	}

	// Batch up the diffs
	// TODO BatchTableWritesSync should batch []Mutation into []Mutation instead
	batches, err := BatchTableWritesSync(diffs)
	if err != nil {
		err = errors.WithStack(err)
		return
	}

	writeCount := 0

	// Write every batch
	for _, batch := range batches {
		writeCount += len(batch.Rows)
		// TODO we may need to split up batches across multiple transactions...?
		m := toMutation(batch)
		var batchRowCount int
		var batchBytes uint64
		batchRowCount, batchBytes, err = m.Write(ctx, tx)
		if err != nil {
			err = errors.WithStack(err)
			return
		}
		rowCount += batchRowCount
		sizeBytes += batchBytes
	}

	chunksProcessed.WithLabelValues(m.Table.Name).Inc()
	rowsProcessed.WithLabelValues(m.Table.Name).Add(float64(len(m.Rows)))

	return rowCount, sizeBytes, nil
}

func toMutation(batch Batch) Mutation {
	rows := make([][]interface{}, len(batch.Rows))
	for i, row := range batch.Rows {
		rows[i] = row.Data
	}
	return Mutation{
		Type:  batch.Type,
		Table: batch.Table,
		Rows:  rows,
	}
}
