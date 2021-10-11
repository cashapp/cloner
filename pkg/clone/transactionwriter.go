package clone

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/mightyguava/autotx"
	"github.com/mitchellh/hashstructure/v2"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	_ "net/http/pprof"
	"reflect"
	"strings"
	"time"
)

type DBWriter interface {
	DBReader
	ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error)
}

// TransactionWriter receives transactions and requests to snapshot and writes transactions and strongly consistent chunk snapshots
type TransactionWriter struct {
	config Replicate
	target *sql.DB

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

type pkSet struct {
	table *mysqlschema.Table
	m     map[uint64][][]interface{}
}

// Intersects checks if any PKs in the set belong to the chunk
func (s *pkSet) Intersects(chunk Chunk) bool {
	for _, pks := range s.m {
		for _, pk := range pks {
			if chunk.ContainsPKs(pk) {
				return true
			}
		}
	}
	return false
}

func (s *pkSet) ContainsRow(row []interface{}) bool {
	pkValues, err := s.table.GetPKValues(row)
	if err != nil {
		panic(err)
	}
	return s.ContainsPK(pkValues)
}

func (s *pkSet) ContainsPK(pks []interface{}) bool {
	hash, err := hashstructure.Hash(pks, hashstructure.FormatV2, nil)
	if err != nil {
		panic(err)
	}
	vals, exists := s.m[hash]
	if !exists {
		return false
	}
	for _, val := range vals {
		if reflect.DeepEqual(pks, val) {
			return true
		}
	}
	return false
}

func (s *pkSet) AddRow(row []interface{}) {
	pkValues, err := s.table.GetPKValues(row)
	if err != nil {
		panic(err)
	}
	s.AddPK(pkValues)
}

func (s *pkSet) AddPK(pks []interface{}) {
	hash, err := hashstructure.Hash(pks, hashstructure.FormatV2, nil)
	if err != nil {
		panic(err)
	}
	s.m[hash] = append(s.m[hash], pks)
}

type transactionSequence struct {
	writer *TransactionWriter
	// chunks is the list of chunks this sequence repair
	chunks []Chunk
	// primaryKeys caches the primary key sets of this sequence, keyed by table name
	primaryKeys  map[string]*pkSet
	transactions []Transaction
}

func (s *transactionSequence) Print(ctx context.Context) {
	writer := &printingWriter{db: s.writer.target}
	for _, transaction := range s.transactions {
		fmt.Printf("---\n")
		for _, mutation := range transaction.Mutations {
			_ = mutation.Write(ctx, writer)
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
			for _, row := range mutation.Rows {
				if pks.ContainsRow(row) {
					return true
				}
			}
		}
	}
	return false
}

func (s *transactionSequence) Append(transaction Transaction) {
	s.transactions = append(s.transactions, transaction)

	// update the cache of the primary key set
	for _, mutation := range transaction.Mutations {
		if mutation.Type == Repair {
			s.chunks = append(s.chunks, mutation.Chunk)
		} else {
			pks, exists := s.primaryKeys[mutation.Table.Name]
			if !exists {
				pks = &pkSet{
					table: mutation.Table.MysqlTable,
					m:     make(map[uint64][][]interface{}),
				}
				s.primaryKeys[mutation.Table.Name] = pks
			}
			for _, row := range mutation.Rows {
				pks.AddRow(row)
			}
		}
	}
}

func (s *transactionSequence) Run(ctx context.Context) error {
	for _, transaction := range s.transactions {
		if len(transaction.Mutations) == 0 {
			continue
		}
		err := autotx.Transact(ctx, s.writer.target, func(tx *sql.Tx) error {
			for _, mutation := range transaction.Mutations {
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

type transactionSet struct {
	writer *TransactionWriter

	sequences     []*transactionSequence
	finalPosition Position
	g             *errgroup.Group
}

func (s *transactionSet) Append(transaction Transaction) {
	s.finalPosition = transaction.FinalPosition
	for _, sequence := range s.sequences {
		if sequence.IsCausal(transaction) {
			sequence.Append(transaction)
			return
		}
	}

	// Non-causal with any of the existing sequences so we create a new sequence for this transaction
	sequence := &transactionSequence{writer: s.writer, primaryKeys: make(map[string]*pkSet)}
	sequence.Append(transaction)
	s.sequences = append(s.sequences, sequence)
}

func (s *transactionSet) Wait() error {
	return errors.WithStack(s.g.Wait())
}

func (s *transactionSet) Start(parent context.Context) {
	if s.g != nil {
		panic("can't start twice")
	}
	g, ctx := errgroup.WithContext(parent)
	s.g = g
	writerParallelism := semaphore.NewWeighted(s.writer.config.ReplicationParallelism)
	for _, seq := range s.sequences {
		sequence := seq
		s.g.Go(func() error {
			err := writerParallelism.Acquire(ctx, 1)
			if err != nil {
				return errors.WithStack(err)
			}
			defer writerParallelism.Release(1)
			err = sequence.Run(ctx)
			if err != nil {
				if isWriteConflict(err) {
					fmt.Printf("write conflict when committing this sequence:\n")
					sequence.Print(parent)
					fmt.Printf("all sequences:\n")
					s.Print(parent)
				}
				return errors.WithStack(err)
			}
			return nil
		})
	}
}

type printingWriter struct {
	db *sql.DB
}

func (l *printingWriter) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	fmt.Printf("%v %v\n", query, args)
	return l.db.QueryContext(ctx, query, args...)
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
			err = autotx.Transact(ctx, w.target, func(tx *sql.Tx) error {
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
	// TODO these should probably be configurable? or auto tuning?
	transactionSetTimeoutDuration := 5 * time.Second
	transactionSetMaxSize := 100

	size := 0
	nextTransactionSet := &transactionSet{writer: w}
	transactionSetTimeout := time.After(transactionSetTimeoutDuration)

	// Fill the next transaction set before the transaction set timeout expires
	for {
		select {
		case transaction := <-transactions:
			size++
			nextTransactionSet.Append(transaction)
			if size >= transactionSetMaxSize {
				return nextTransactionSet, nil
			}
		case <-transactionSetTimeout:
			if size == 0 {
				// We have no transactions so we might as well wait a bit longer
				transactionSetTimeout = time.After(transactionSetTimeoutDuration)
				continue
			}
			return nextTransactionSet, nil
		case <-ctx.Done():
			return nextTransactionSet, ctx.Err()
		}
	}
}

func (w *TransactionWriter) handleMutation(ctx context.Context, tx *sql.Tx, mutation Mutation) error {
	if mutation.Table.Name == w.config.WatermarkTable {
		// We don't send writes to the watermark table to the target
		return nil
	}
	err := mutation.Write(ctx, tx)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (m *Mutation) Write(ctx context.Context, tx DBWriter) error {
	var err error
	switch m.Type {
	case Repair:
		err = m.repair(ctx, tx)
	case Delete:
		err = m.delete(ctx, tx)
	case Insert, Update:
		err = m.replace(ctx, tx)
	default:
		panic(fmt.Sprintf("unknown mutation type: %d", m.Type))
	}
	return errors.WithStack(err)
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
			writesFailed.WithLabelValues(tableName, writeType).Add(float64(len(m.Rows)))
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

	valueStrings := make([]string, 0, len(m.Rows))
	valueArgs := make([]interface{}, 0, len(m.Rows)*len(tableSchema.Columns))
	for _, row := range m.Rows {
		valueStrings = append(valueStrings, values)
		valueArgs = append(valueArgs, row...)
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
			writesFailed.WithLabelValues(tableName, writeType).Add(float64(len(m.Rows)))
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
			name      VARCHAR(255) NOT NULL,
			file      VARCHAR(255) NOT NULL,
			position  BIGINT(20)   NOT NULL,
			gtid_set  TEXT,
			timestamp TIMESTAMP    NOT NULL,
			PRIMARY KEY (name)
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
	_, err := tx.ExecContext(ctx,
		fmt.Sprintf("REPLACE INTO %s (name, file, position, gtid_set, timestamp) VALUES (?, ?, ?, ?, ?)",
			w.config.CheckpointTable),
		w.config.TaskName, position.File, position.Position, gsetString, time.Now().UTC())
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

// repair synchronously diffs and writes the chunk to the target (diff and write)
// the writes are made synchronously in the replication stream to maintain strong consistency
func (m *Mutation) repair(ctx context.Context, tx DBWriter) error {
	targetStream, err := readChunk(ctx, tx, "target", m.Chunk)
	if err != nil {
		return errors.WithStack(err)
	}

	// Diff the streams
	// TODO StreamDiff should return []Mutation here instead
	diffs, err := StreamDiff(ctx, m.Table, stream(m.Table, m.Rows), targetStream)
	if err != nil {
		return errors.WithStack(err)
	}

	if len(diffs) > 0 {
		chunksWithDiffs.WithLabelValues(m.Table.Name).Inc()
	}

	// Batch up the diffs
	// TODO BatchTableWritesSync should batch []Mutation into []Mutation instead
	batches, err := BatchTableWritesSync(diffs)
	if err != nil {
		return errors.WithStack(err)
	}

	writeCount := 0

	// Write every batch
	for _, batch := range batches {
		writeCount += len(batch.Rows)
		// TODO we may need to split up batches across multiple transactions...?
		m := toMutation(batch)
		err := m.Write(ctx, tx)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	chunksProcessed.WithLabelValues(m.Table.Name).Inc()
	rowsProcessed.WithLabelValues(m.Table.Name).Add(float64(len(m.Rows)))

	return nil
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
