package clone

import (
	"context"
	"database/sql"
	"fmt"
	"math/rand"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

var (
	tablesTotalMetric = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "tables",
			Help: "How many total tables to do.",
		},
	)
	rowCountMetric = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "estimated_rows",
			Help: "How many total rows to do.",
		},
		[]string{"table"},
	)
	tablesDoneMetric = prometheus.NewGauge(
		prometheus.GaugeOpts{
			Name: "tables_done",
			Help: "How many tables done.",
		},
	)
)

func init() {
	prometheus.MustRegister(tablesTotalMetric)
	prometheus.MustRegister(rowCountMetric)
	prometheus.MustRegister(tablesDoneMetric)
}

type Reader struct {
	config ReaderConfig
	table  *Table
	source *sql.DB
	target *sql.DB

	sourceRetry RetryOptions
	targetRetry RetryOptions
	speedLogger *ThroughputLogger
}

func (r *Reader) Diff(ctx context.Context, diffs chan Diff) error {
	return errors.WithStack(r.read(ctx, diffs, true))
}

func (r *Reader) Read(ctx context.Context, diffs chan Diff) error {
	// TODO this can be refactored to a separate method
	return errors.WithStack(r.read(ctx, diffs, false))
}

func (r *Reader) read(ctx context.Context, diffsCh chan Diff, diff bool) error {
	g, ctx := errgroup.WithContext(ctx)
	g.SetLimit(r.config.ReaderParallelism)

	chunkCh := make(chan Chunk)

	// Generate chunks of source table
	g.Go(func() error {
		if r.config.ShuffleChunks {
			chunks, err := generateTableChunks(ctx, r.table, r.source, r.sourceRetry)
			if err != nil {
				return errors.WithStack(err)
			}
			rand.Shuffle(len(chunks), func(i, j int) { chunks[i], chunks[j] = chunks[j], chunks[i] })
			for _, chunk := range chunks {
				select {
				case chunkCh <- chunk:
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		} else {
			err := generateTableChunksAsync(ctx, r.table, r.source, chunkCh, r.sourceRetry)
			if err != nil {
				return errors.WithStack(err)
			}
		}
		close(chunkCh)
		return nil
	})

	logger := log.WithContext(ctx).WithField("task", "reader")
	logger = logger.WithField("table", r.table.Name)

	// Generate diffs from all chunks
	chunkCount := 0
	rowCount := 0
	for c := range chunkCh {
		chunk := c
		chunkCount += 1
		rowCount += c.Size
		g.Go(func() (err error) {
			return r.processChunk(ctx, diffsCh, diff, chunk)
		})
	}

	err := g.Wait()
	if err != nil {
		return errors.WithStack(err)
	}

	logger.Infof("reads done: %s (chunks=%d rows=%d)", r.table.Name, chunkCount, rowCount)

	return nil
}

func (r *Reader) processChunk(ctx context.Context, diffsCh chan Diff, diff bool, chunk Chunk) (err error) {
	var diffs []Diff
	if diff {
		diffs, err = r.diffChunk(ctx, chunk)
	} else {
		diffs, err = r.readChunk(ctx, chunk)
	}

	if err != nil {
		log.WithField("table", chunk.Table.Name).
			WithError(err).
			WithContext(ctx).
			Warnf("failed to read chunk %s[%v - %v] after retries and backoff, "+
				"since this is a best effort clone we just give up: %+v",
				chunk.Table.Name, chunk.Start, chunk.End, err)
		return nil
	}

	if len(diffs) > 0 {
		chunksWithDiffs.WithLabelValues(chunk.Table.Name).Inc()
	}

	chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()

	for _, diff := range diffs {
		select {
		case diffsCh <- diff:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

func (r *Reader) diffChunkWithTableLock(ctx context.Context, chunk Chunk) (diffs []Diff, err error) {
	log.Infof("diffing chunk %v with table lock, max duration %v", chunk.String(), r.config.TableLockMaxDuration)

	startTime := time.Now()
	lockConn, err := r.lockTable(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer func(r *Reader, ctx context.Context, lockConn *sql.Conn) {
		err := r.unlockTables(ctx, lockConn)
		if err != nil {
			log.WithError(err).Warnf("failed to unlock table %v: %v", r.table.Name, err)
			_ = lockConn.Close()
		}
	}(r, ctx, lockConn)
	// Other connections can still read the table (just not write), so we don't have to use the lockConn to do the diff

	tries := 0
	timeoutCtx, cancelFunc := context.WithTimeout(ctx, r.config.TableLockMaxDuration)
	defer cancelFunc()
	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.InitialInterval = time.Millisecond
	exponentialBackOff.MaxInterval = 3 * time.Second
	exponentialBackOff.MaxElapsedTime = r.config.TableLockMaxDuration
	backOff := backoff.WithContext(
		backoff.WithMaxRetries(
			exponentialBackOff, uint64(r.config.FailedChunkRetryCount)),
		timeoutCtx)
	err = backoff.Retry(func() error {
		diffs = nil
		diffs, err = r.doDiffChunk(ctx, chunk)
		if err != nil {
			return errors.WithStack(err)
		}
		if len(diffs) == 0 {
			if tries > 1 {
				log.Infof("chunk %s[%v-%v) had no diffs after %d retries",
					chunk.Table.Name, chunk.Start, chunk.End, tries)
			}
			// Yay! Chunk had no diffs!!
			return nil
		} else {
			if r.config.FailedChunkRetryCount-tries > 0 {
				log.Infof("chunk %s[%d-%d) had diffs, retrying %d more times",
					chunk.Table.Name, chunk.Start, chunk.End, r.config.FailedChunkRetryCount-tries)
				tries++
			}
			return &ChunkRetryError{Chunk: chunk}
		}
	}, backOff)
	if errors.Is(err, &ChunkRetryError{}) {
		err = nil
	}
	lockDuration := time.Now().Sub(startTime)
	if len(diffs) > 0 {
		log.Infof("%d diffs found still in chunk %v after diffing with table lock, total lock duration: %v", len(diffs), chunk.String(), lockDuration)
	} else {
		log.Infof("no diffs found in chunk %v after diffing with table lock, total lock duration: %v", chunk.String(), lockDuration)
	}
	return diffs, nil
}

func (r *Reader) lockTable(ctx context.Context) (*sql.Conn, error) {
	log.Infof("locking table for reads only: %v", r.table.Name)
	conn, err := r.source.Conn(ctx)
	_, err = conn.ExecContext(ctx, fmt.Sprintf("LOCK TABLE `%s` READ", r.table.Name))
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return conn, err
}

// unlockTables unlocks all tables and closes the connection
func (r *Reader) unlockTables(ctx context.Context, conn *sql.Conn) error {
	log.Infof("unlocking table: %v", r.table.Name)
	defer conn.Close()
	conn, err := r.source.Conn(ctx)
	_, err = conn.ExecContext(ctx, "UNLOCK TABLES")
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func NewReader(
	config ReaderConfig,
	table *Table,
	speedLogger *ThroughputLogger,
	source *sql.DB,
	sourceLimiter core.Limiter,
	target *sql.DB,
	targetLimiter core.Limiter,
) *Reader {
	return &Reader{
		config:      config,
		table:       table,
		source:      source,
		speedLogger: speedLogger,
		sourceRetry: RetryOptions{
			Limiter:       sourceLimiter,
			AcquireMetric: readLimiterDelay.WithLabelValues("source"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
		target: target,
		targetRetry: RetryOptions{
			Limiter:       targetLimiter,
			AcquireMetric: readLimiterDelay.WithLabelValues("target"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
	}
}
