package clone

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"go.uber.org/atomic"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	_ "net/http/pprof"
	"sort"
)

// Snapshotter receives transactions and requests to snapshot and writes transactions and strongly consistent chunk snapshots
type Snapshotter struct {
	config       Replicate
	source       *sql.DB
	sourceSchema string
	target       *sql.DB

	sourceRetry RetryOptions
	targetRetry RetryOptions

	// chunks receives a channel of chunks when a snapshot starts
	chunks chan chan Chunk

	// ongoingChunks holds the currently ongoing chunks, only access from the replication thread
	ongoingChunks []*ChunkSnapshot

	// snapshotRunning is true while a snapshot is running
	snapshotRunning *atomic.Bool
}

func NewSnapshotter(config Replicate) (*Snapshotter, error) {
	var err error
	r := Snapshotter{
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
		snapshotRunning: atomic.NewBool(false),
		chunks:          make(chan chan Chunk),
	}
	r.sourceSchema, err = r.config.Source.Schema()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	source, err := r.config.Source.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.source = source

	target, err := r.config.Target.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.target = target

	return &r, nil
}

func (s *Snapshotter) Init(ctx context.Context) error {
	err := s.source.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	err = s.target.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	if s.config.CreateTables {
		err = s.createWatermarkTable(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (s *Snapshotter) Run(ctx context.Context, b backoff.BackOff, source chan Transaction, sink chan Transaction) error {
	logger := logrus.WithField("task", "snapshotter")

	var chunks chan Chunk
	for {
		select {
		case chunks = <-s.chunks:
			logger.Infof("snapshot starting")
		default:
		}
		if chunks != nil {
			done, err := s.maybeSnapshotChunks(ctx, chunks)
			if err != nil {
				return errors.WithStack(err)
			}
			if done {
				chunks = nil
			}
		}
		if chunks == nil && len(s.ongoingChunks) == 0 && s.snapshotRunning.Load() {
			logger.Infof("snapshot done")
			chunksEnqueued.Reset()
			chunksProcessed.Reset()
			rowsProcessed.Reset()
			chunksWithDiffs.Reset()
			s.snapshotRunning.Store(false)
		}

		var transaction Transaction
		select {
		case transaction = <-source:
		case <-ctx.Done():
			return ctx.Err()
		}

		// If we have chunks to process then we will process the watermarks in the transactions
		if len(s.ongoingChunks) > 0 {
			newMutations := make([]Mutation, 0, len(transaction.Mutations))
			for _, mutation := range transaction.Mutations {
				err := s.reconcileOngoingChunks(mutation)
				if err != nil {
					return errors.WithStack(err)
				}
				newMutations, err = s.handleWatermark(mutation, newMutations)
				if err != nil {
					return errors.WithStack(err)
				}
			}
			transaction.Mutations = newMutations
		}

		select {
		case sink <- transaction:
		case <-ctx.Done():
			return ctx.Err()
		}

		// We've committed a transaction, we can reset the backoff
		b.Reset()
	}
}

func (s *Snapshotter) createWatermarkTable(ctx context.Context) error {
	// TODO retries with backoff?
	timeoutCtx, cancel := context.WithTimeout(ctx, s.config.WriteTimeout)
	defer cancel()
	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id         BIGINT(20)   NOT NULL AUTO_INCREMENT,
			table_name VARCHAR(255) NOT NULL,
			chunk_seq  BIGINT(20)   NOT NULL,
			low        TINYINT      DEFAULT 0,
			high       TINYINT      DEFAULT 0,
			PRIMARY KEY (id)
		)
		`, s.config.WatermarkTable)
	_, err := s.source.ExecContext(timeoutCtx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not create checkpoint table in target database:\n%s", stmt)
	}
	return nil
}

// ChunkSnapshot is a mutable struct for representing the current reconciliation state of a chunk, it is used single
// threaded by the replication thread only
type ChunkSnapshot struct {
	InsideWatermarks bool
	Rows             []*Row
	Chunk            Chunk
}

func (c *ChunkSnapshot) findRow(row []interface{}) (*Row, int, error) {
	n := len(c.Rows)
	i := sort.Search(n, func(i int) bool {
		return c.Rows[i].PkAfterOrEqual(row)
	})
	var candidate *Row
	if n == i {
		i = -1
	} else {
		candidate = c.Rows[i]
		if !candidate.PkEqual(row) {
			candidate = nil
		}
	}
	return candidate, i, nil
}

func (c *ChunkSnapshot) updateRow(i int, row []interface{}) {
	c.Rows[i] = c.Rows[i].Updated(row)
}

func (c *ChunkSnapshot) deleteRow(i int) {
	c.Rows = append(c.Rows[:i], c.Rows[i+1:]...)
}

func (c *ChunkSnapshot) insertRow(i int, row []interface{}) {
	pk := c.Chunk.Table.PkOfRow(row)
	r := &Row{
		Table: c.Chunk.Table,
		ID:    pk,
		Data:  row,
	}
	if i == -1 {
		// We found no place to insert it so we append it
		c.Rows = append(c.Rows, r)
	} else {
		c.Rows = append(c.Rows[:i], append([]*Row{r}, c.Rows[i:]...)...)
	}
}

// reconcileOngoingChunks reconciles any ongoing chunks with the changes in the binlog event
func (s *Snapshotter) reconcileOngoingChunks(mutation Mutation) error {
	// should be O(<rows in the RowsEvent> * lg <rows in the chunk>) given that we can binary chop into chunk
	// RowsEvent is usually not that large so I don't think we need to index anything, that will probably be slower
	for _, chunk := range s.ongoingChunks {
		err := chunk.reconcileBinlogEvent(mutation)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

// reconcileBinlogEvent will apply the row
func (c *ChunkSnapshot) reconcileBinlogEvent(mutation Mutation) error {
	tableSchema := mutation.Table.MysqlTable
	if !c.InsideWatermarks {
		return nil
	}
	if c.Chunk.Table.MysqlTable.Name != tableSchema.Name {
		return nil
	}
	if c.Chunk.Table.MysqlTable.Schema != tableSchema.Schema {
		return nil
	}
	if mutation.Type == Delete {
		for _, row := range mutation.Rows {
			if !c.Chunk.ContainsRow(row) {
				// The row is outside of our range, we can skip it
				continue
			}
			// find the row using binary chop (the chunk rows are sorted)
			existingRow, i, err := c.findRow(row)
			if existingRow == nil {
				// Row already deleted, this event probably happened after the low watermark but before the chunk read
			} else {
				if err != nil {
					return errors.WithStack(err)
				}
				c.deleteRow(i)
			}
		}
	} else {
		for _, row := range mutation.Rows {
			if !c.Chunk.ContainsRow(row) {
				// The row is outside of our range, we can skip it
				continue
			}
			existingRow, i, err := c.findRow(row)
			if err != nil {
				return errors.WithStack(err)
			}
			if existingRow == nil {
				// This is either an insert or an update of a row that is deleted after the low watermark but before
				// the chunk read, either way we just insert it and if the delete event comes we take it away again
				c.insertRow(i, row)
			} else {
				// We found a matching row, it must be an update
				c.updateRow(i, row)
			}
		}
	}
	return nil
}

func (s *Snapshotter) findOngoingChunkFromWatermark(mutation Mutation) (*ChunkSnapshot, error) {
	logger := logrus.WithField("task", "snapshotter")

	tableSchema := mutation.Table.MysqlTable
	tableNameI, err := tableSchema.GetColumnValue("table_name", mutation.Rows[0])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	tableName := tableNameI.(string)
	chunkSeqI, err := tableSchema.GetColumnValue("chunk_seq", mutation.Rows[0])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	chunkSeq := chunkSeqI.(int64)
	for _, chunk := range s.ongoingChunks {
		if chunk.Chunk.Seq == chunkSeq && chunk.Chunk.Table.Name == tableName {
			return chunk, nil
		}
	}
	logger.Warnf("could not find chunk for watermark for table '%s', "+
		"we may be receiving the watermark events before the chunk "+
		"or this is a watermark left behind from an earlier failed snapshot "+
		"attempt that crashed before it completed", tableName)
	return nil, nil
}

func (s *Snapshotter) removeOngoingChunk(chunk *ChunkSnapshot) {
	logger := logrus.WithField("task", "replicate")

	n := 0
	for _, x := range s.ongoingChunks {
		if x != chunk {
			s.ongoingChunks[n] = x
			n++
		}
	}
	s.ongoingChunks = s.ongoingChunks[:n]
	if chunk.Chunk.Last {
		logger.WithField("table", chunk.Chunk.Table.Name).
			Infof("'%s' snapshot done", chunk.Chunk.Table.Name)
	}
}

// snapshot runs a snapshot asynchronously unless a snapshot is already running
func (s *Snapshotter) snapshot(ctx context.Context) error {
	succeeded := s.snapshotRunning.CAS(false, true)
	if !succeeded {
		// Someone else won the race
		return nil
	}

	go func() {
		logger := logrus.WithContext(ctx).WithField("task", "chunking")
		err := s.chunkTables(ctx)
		if err != nil {
			logger.WithError(err).Errorf("failed to chunk tables: %v", err)
		}
	}()

	return nil
}

func (s *Snapshotter) handleWatermark(mutation Mutation, result []Mutation) ([]Mutation, error) {
	if mutation.Table.Name != s.config.WatermarkTable {
		// Nothing to do, we just add the mutation untouched and return
		result = append(result, mutation)
		return result, nil
	}
	if mutation.Type == Delete {
		// Someone is probably just cleaning out the watermark table
		// Watermark mutations aren't replicated so we don't bother adding it
		return result, nil
	}
	if len(mutation.Rows) != 1 {
		return result, errors.Errorf("more than a single row was written to the watermark table at the same time")
	}
	row := mutation.Rows[0]
	low, err := mutation.Table.MysqlTable.GetColumnValue("low", row)
	if err != nil {
		return result, errors.WithStack(err)
	}
	high, err := mutation.Table.MysqlTable.GetColumnValue("high", row)
	if err != nil {
		return result, errors.WithStack(err)
	}
	if low.(int8) == 1 {
		ongoingChunk, err := s.findOngoingChunkFromWatermark(mutation)
		if err != nil {
			return result, errors.WithStack(err)
		}
		if ongoingChunk == nil {
			return result, nil
		}
		ongoingChunk.InsideWatermarks = true
	}
	if high.(int8) == 1 {
		ongoingChunk, err := s.findOngoingChunkFromWatermark(mutation)
		if err != nil {
			return result, errors.WithStack(err)
		}
		if ongoingChunk == nil {
			return result, nil
		}

		ongoingChunk.InsideWatermarks = false
		rows := make([][]interface{}, len(ongoingChunk.Rows))
		for i, row := range ongoingChunk.Rows {
			rows[i] = row.Data
		}
		result = append(result, Mutation{
			Type:  Repair,
			Table: ongoingChunk.Chunk.Table,
			Rows:  rows,
			Chunk: ongoingChunk.Chunk,
		})
		s.removeOngoingChunk(ongoingChunk)
	}
	return result, nil
}

func (s *Snapshotter) chunkTables(ctx context.Context) error {
	chunks := make(chan Chunk, s.config.ChunkBufferSize)

	tables, err := loadTables(ctx, s.config.ReaderConfig, s.config.Source, s.source)
	if err != nil {
		return errors.WithStack(err)
	}

	g, ctx := errgroup.WithContext(ctx)

	tableParallelism := semaphore.NewWeighted(s.config.TableParallelism)

	logger := logrus.WithContext(ctx).WithField("task", "chunking")

	g.Go(func() error {
		s.chunks <- chunks
		return nil
	})

	for _, t := range tables {
		table := t
		err = tableParallelism.Acquire(ctx, 1)
		if err != nil {
			return errors.WithStack(err)
		}
		g.Go(func() error {
			defer tableParallelism.Release(1)

			logger := logger.WithField("table", table.Name)
			logger.Infof("'%s' chunking start", table.Name)
			err := generateTableChunksAsync(ctx, table, s.source, chunks, s.sourceRetry)
			logger.Infof("'%s' chunking done", table.Name)
			if err != nil {
				return errors.Wrapf(err, "failed to chunk: '%s'", table.Name)
			}

			return nil
		})
	}

	err = g.Wait()
	logger.Infof("table chunking done")

	close(chunks)
	return errors.WithStack(err)
}

func (s *Snapshotter) snapshotChunk(ctx context.Context, chunk Chunk) (*ChunkSnapshot, error) {
	//   1. insert the low watermark
	//   2. read the entire chunk
	//   3. insert the high watermark

	_, err := s.source.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (table_name, chunk_seq, low) VALUES (?, ?, 1)",
			s.config.WatermarkTable),
		chunk.Table.Name, chunk.Seq)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	stream, err := bufferChunk(ctx, s.sourceRetry, s.source, "source", chunk)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	rows, err := readAll(stream)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	snapshot := &ChunkSnapshot{Chunk: chunk, Rows: rows}

	chunksSnapshotted.WithLabelValues(chunk.Table.Name).Inc()

	_, err = s.source.ExecContext(ctx,
		fmt.Sprintf("INSERT INTO %s (table_name, chunk_seq, high) VALUES (?, ?, 1)",
			s.config.WatermarkTable),
		chunk.Table.Name, chunk.Seq)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return snapshot, nil
}

func (s *Snapshotter) maybeSnapshotChunks(ctx context.Context, chunkCh chan Chunk) (bool, error) {
	// We read new snapshot when we have finished processing all of the ongoing chunks
	if len(s.ongoingChunks) > 0 {
		return false, nil
	}

	g, ctx := errgroup.WithContext(ctx)

	snapshotCh := make(chan *ChunkSnapshot, s.config.ChunkParallelism)

	closed := atomic.NewBool(false)

	for i := 0; i < s.config.ChunkParallelism; i++ {
		g.Go(func() error {
			select {
			case chunk, isOpen := <-chunkCh:
				if !isOpen {
					// Channel is closed, we're done with all the chunks
					closed.Store(true)
					return nil
				}
				snapshot, err := s.snapshotChunk(ctx, chunk)
				snapshotCh <- snapshot
				return errors.WithStack(err)
			case <-ctx.Done():
				return ctx.Err()
			}
		})
	}

	err := g.Wait()
	if err != nil {
		return false, errors.WithStack(err)
	}
	close(snapshotCh)

	for chunk := range snapshotCh {
		s.ongoingChunks = append(s.ongoingChunks, chunk)
	}
	done := closed.Load()
	return done, nil
}
