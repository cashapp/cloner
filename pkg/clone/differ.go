package clone

import (
	"context"
	"fmt"
	"strings"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type MutationType byte

const (
	Insert MutationType = iota
	Update
	Delete

	// Repair is a mutation which sends a full chunk which is then diffed against the target and the diffs are applied
	Repair
)

func (m MutationType) String() string {
	switch m {
	case Insert:
		return "insert"
	case Update:
		return "update"
	case Delete:
		return "delete"
	case Repair:
		return "repair"
	}
	return "unknown"
}

var (
	chunksEnqueued = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chunks_enqueued",
			Help: "How many chunks has been enqueued, partitioned by table.",
		},
		[]string{"table"},
	)
	chunksProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chunks_processed",
			Help: "How many chunks has been processed, partitioned by table.",
		},
		[]string{"table"},
	)
	chunksWithDiffs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chunks_with_diffs",
			Help: "How many chunks have diffs, partitioned by table.",
		},
		[]string{"table"},
	)
	diffCount = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "diff_count",
			Help: "How many diffs (rows to delete/insert/update).",
		},
		[]string{"table", "type"},
	)
	readsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reads_processed",
			Help: "How many rows read by table.",
		},
		[]string{"table", "side"},
	)
	rowsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "rows_processed",
			Help: "How many rows processed on the source side (chunks processed times chunk size).",
		},
		[]string{"table"},
	)
	readsBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reads_bytes",
			Help: "How many bytes we have read from the source, partitioned by table.",
		},
		[]string{"table"},
	)
	readDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "read_duration",
			Help:    "Total duration of the database read of a chunk from a table from either source or target.",
			Buckets: defaultBuckets,
		},
		[]string{"table", "from"},
	)
	crc32Duration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "crc32_duration",
			Help:    "Duration of running the crc32 pre-diffing check.",
			Buckets: defaultBuckets,
		},
		[]string{"table", "from"},
	)
	diffDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "diff_duration",
			Help:    "Total duration of diffing a chunk (including database reads).",
			Buckets: defaultBuckets,
		},
		[]string{"table"},
	)
	readLimiterDelay = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "read_limiter_delay_duration",
			Help:    "Duration of back off from the concurrency limiter for reads.",
			Buckets: defaultBuckets,
		},
		[]string{"from"},
	)
)

func init() {
	prometheus.MustRegister(readsProcessed)
	prometheus.MustRegister(chunksEnqueued)
	prometheus.MustRegister(chunksProcessed)
	prometheus.MustRegister(readsBytes)
	prometheus.MustRegister(rowsProcessed)
	prometheus.MustRegister(chunksWithDiffs)
	prometheus.MustRegister(diffCount)
	prometheus.MustRegister(readDuration)
	prometheus.MustRegister(readLimiterDelay)
	prometheus.MustRegister(crc32Duration)
	prometheus.MustRegister(diffDuration)
}

type Diff struct {
	Type MutationType
	// Row is the row to update to or insert or delete
	Row *Row

	// Target is in case of the Update MutationType also set so that it can be compared
	Target *Row
}

// StreamDiff returns the changes need to make target become exactly like source
func StreamDiff(ctx context.Context, table *Table, source RowStream, target RowStream) ([]Diff, error) {
	var err error

	chunksWithDiffs := chunksWithDiffs.WithLabelValues(table.Name)

	hasDiff := false
	defer func() {
		if hasDiff {
			chunksWithDiffs.Inc()
		}
	}()
	advanceSource := true
	advanceTarget := true

	var result []Diff

	var sourceRow *Row
	var targetRow *Row
	for {
		if advanceSource {
			sourceRow, err = source.Next()
			if err != nil {
				return result, errors.WithStack(err)
			}
			readsProcessed.WithLabelValues(table.Name, "source").Inc()
		}
		if advanceTarget {
			targetRow, err = target.Next()
			if err != nil {
				return result, errors.WithStack(err)
			}
			readsProcessed.WithLabelValues(table.Name, "target").Inc()
		}
		advanceSource = false
		advanceTarget = false

		if sourceRow != nil {
			if targetRow != nil {
				comparison := genericCompareKeys(sourceRow.KeyValues(), targetRow.KeyValues())
				if comparison < 0 {
					result = append(result, Diff{Insert, sourceRow, nil})
					diffCount.WithLabelValues(table.Name, "insert").Inc()
					hasDiff = true
					advanceSource = true
					advanceTarget = false
				} else if comparison > 0 {
					result = append(result, Diff{Delete, targetRow, nil})
					diffCount.WithLabelValues(table.Name, "delete").Inc()
					hasDiff = true
					advanceSource = false
					advanceTarget = true
				} else {
					isEqual, err := RowsEqual(sourceRow, targetRow)
					if err != nil {
						return result, errors.WithStack(err)
					}
					if !isEqual {
						result = append(result, Diff{Update, sourceRow, targetRow})
						hasDiff = true
						diffCount.WithLabelValues(table.Name, "update").Inc()
						advanceSource = true
						advanceTarget = true
					} else {
						// Same!
						advanceSource = true
						advanceTarget = true
					}
				}
			} else {
				result = append(result, Diff{Insert, sourceRow, nil})
				diffCount.WithLabelValues(table.Name, "insert").Inc()
				hasDiff = true
				advanceSource = true
			}
		} else if targetRow != nil {
			hasDiff = true
			result = append(result, Diff{Delete, targetRow, nil})
			diffCount.WithLabelValues(table.Name, "delete").Inc()
			hasDiff = true
			advanceTarget = true
		} else {
			return result, nil
		}
	}
}

func RowsEqual(sourceRow *Row, targetRow *Row) (bool, error) {
	for i := range sourceRow.Data {
		equals, err := genericEquals(sourceRow.Data[i], targetRow.Data[i])
		if err != nil {
			return false, errors.WithStack(err)
		}
		if !equals {
			return false, nil
		}
	}
	return true, nil
}

// mysqlTimeFormat is the standard time format recognized by MySQL
// note that it does not contain timezone, it will be interpreted in the configured timezone of the server
// if this is used to copy data between databases we need to make sure they are using the same timezone
const mysqlTimeFormat = "2006-01-02 15:04:05"

// readChunk reads a chunk without diffing producing only insert diffs
func (r *Reader) readChunk(ctx context.Context, chunk Chunk) ([]Diff, error) {
	var sizeBytes uint64
	timer := prometheus.NewTimer(diffDuration.WithLabelValues(chunk.Table.Name))
	defer func() {
		timer.ObserveDuration()
		r.speedLogger.Record(chunk.Table.Name, chunk.Size, sizeBytes)
		chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()
		rowsProcessed.WithLabelValues(chunk.Table.Name).Add(float64(chunk.Size))
	}()

	sourceStream, sizeBytes, err := bufferChunk(ctx, r.sourceRetry, r.source, "source", chunk)
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

func (r *Reader) diffChunk(ctx context.Context, chunk Chunk) ([]Diff, error) {
	// TODO what we should actually do here is do a single pass through all of the successful chunks,
	//      then keep retrying with the failed chunks until they all succeed,
	//      but that will require larger code restructurings so let's wait with that for a bit
	var err error
	if r.config.FailedChunkRetryCount == 0 {
		return r.doDiffChunk(ctx, chunk)
	} else {
		var diffs []Diff
		tries := 0
		err := backoff.Retry(func() error {
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
				return errors.Errorf("chunk %s[%d-%d) had diffs",
					chunk.Table.Name, chunk.Start, chunk.End)
			}
		}, backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), uint64(r.config.FailedChunkRetryCount)), ctx))
		if len(diffs) > 0 {
			return diffs, nil
		} else {
			return diffs, err
		}
	}
}

func (r *Reader) doDiffChunk(ctx context.Context, chunk Chunk) ([]Diff, error) {
	// Make sure we have both a source and target connection before we start to minimize
	// the amount of time in between the reads
	source, err := r.source.Conn(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer source.Close()
	target, err := r.target.Conn(ctx)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer target.Close()

	var sizeBytes uint64
	timer := prometheus.NewTimer(diffDuration.WithLabelValues(chunk.Table.Name))
	defer func() {
		timer.ObserveDuration()
		chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()
		r.speedLogger.Record(chunk.Table.Name, chunk.Size, sizeBytes)
		readsBytes.WithLabelValues(chunk.Table.Name).Add(float64(sizeBytes))
		rowsProcessed.WithLabelValues(chunk.Table.Name).Add(float64(chunk.Size))
	}()

	if r.config.UseCRC32Checksum {
		// start off by running a fast checksum query
		sourceChecksum, err := checksumChunk(ctx, r.sourceRetry, "source", source, chunk)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		targetChecksum, err := checksumChunk(ctx, r.targetRetry, "target", target, chunk)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		if sourceChecksum == targetChecksum {
			// Checksums match, no need to do any further diffing
			return nil, nil
		}
	}

	sourceStream, sizeBytes, err := bufferChunk(ctx, r.sourceRetry, source, "source", chunk)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Sort the snapshot using genericCompare which diff depends on
	sourceStream.sort()
	targetStream, _, err := bufferChunk(ctx, r.targetRetry, target, "target", chunk)
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

func checksumChunk(ctx context.Context, retry RetryOptions, from string, reader DBReader, chunk Chunk) (int64, error) {
	var checksum int64
	err := Retry(ctx, retry, func(ctx context.Context) error {
		timer := prometheus.NewTimer(crc32Duration.WithLabelValues(chunk.Table.Name, from))
		defer timer.ObserveDuration()
		extraWhereClause := ""
		hint := ""
		if from == "target" {
			extraWhereClause = chunk.Table.Config.TargetWhere
			hint = chunk.Table.Config.TargetHint
		}
		if from == "source" {
			extraWhereClause = chunk.Table.Config.SourceWhere
			hint = chunk.Table.Config.SourceHint
		}
		where, params := chunkWhere(chunk, extraWhereClause)
		sql := fmt.Sprintf("SELECT %s BIT_XOR(%s) FROM `%s` %s",
			hint, strings.Join(chunk.Table.CRC32Columns, " ^ "), chunk.Table.Name, where)
		rows, err := reader.QueryContext(ctx, sql, params...)
		if err != nil {
			return errors.WithStack(err)
		}
		defer rows.Close()
		if !rows.Next() {
			return errors.Errorf("no checksum result")
		}
		err = rows.Scan(&checksum)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	return checksum, errors.WithStack(err)
}

// bufferChunk reads and buffers the chunk fully into memory so that we won't time out while diffing even if we have
// to pause due to back pressure from the writer
func bufferChunk(ctx context.Context, retry RetryOptions, source DBReader, from string, chunk Chunk) (*bufferStream, uint64, error) {
	var result *bufferStream
	var sizeBytes uint64

	err := Retry(ctx, retry, func(ctx context.Context) error {
		var err error
		result, sizeBytes, err = readChunk(ctx, source, from, chunk)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	})

	return result, sizeBytes, err
}

// readChunk reads and buffers a chunk without retries
func readChunk(ctx context.Context, source DBReader, from string, chunk Chunk) (*bufferStream, uint64, error) {
	timer := prometheus.NewTimer(readDuration.WithLabelValues(chunk.Table.Name, from))
	defer timer.ObserveDuration()

	extraWhereClause := ""
	hint := ""
	if from == "target" {
		extraWhereClause = chunk.Table.Config.TargetWhere
		hint = chunk.Table.Config.TargetHint
	}
	if from == "source" {
		extraWhereClause = chunk.Table.Config.SourceWhere
		hint = chunk.Table.Config.SourceHint
	}
	stream, err := StreamChunk(ctx, source, chunk, hint, extraWhereClause)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to stream chunk [%d-%d] of %s from %s",
			chunk.Start, chunk.End, chunk.Table.Name, from)
	}
	defer stream.Close()
	result, err := buffer(stream)
	if err != nil {
		return nil, 0, errors.Wrapf(err, "failed to stream chunk [%d-%d] of %s from %s",
			chunk.Start, chunk.End, chunk.Table.Name, from)
	}
	return result, result.SizeBytes(), err
}
