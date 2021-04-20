package clone

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type DiffType string

const (
	Insert DiffType = "insert"
	Update DiffType = "update"
	Delete DiffType = "delete"
)

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
	readDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "read_duration",
			Help:       "Total duration of the database read (including retries and backoff) of a chunk from a table from either source or target.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"table", "from"},
	)
	crc32Duration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "crc32_duration",
			Help:       "Duration of running the crc32 pre-diffing check.",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"table", "from"},
	)
	diffDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "diff_duration",
			Help:       "Total duration of diffing a chunk (including database reads).",
			Objectives: map[float64]float64{0.5: 0.05, 0.9: 0.01, 0.99: 0.001},
		},
		[]string{"table"},
	)
	readLimiterDelay = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name: "read_limiter_delay_duration",
			Help: "Duration of back off from the concurrency limiter for reads.",
		},
		[]string{"from"},
	)
)

func init() {
	prometheus.MustRegister(readsProcessed)
	prometheus.MustRegister(chunksEnqueued)
	prometheus.MustRegister(chunksProcessed)
	prometheus.MustRegister(rowsProcessed)
	prometheus.MustRegister(chunksWithDiffs)
	prometheus.MustRegister(diffCount)
	prometheus.MustRegister(readDuration)
	prometheus.MustRegister(crc32Duration)
	prometheus.MustRegister(diffDuration)
}

type Diff struct {
	Type DiffType
	// Row is the row to update to or insert or delete
	Row *Row

	// Target is in case of the Update DiffType also set so that it can be compared
	Target *Row
}

// StreamDiff sends the changes need to make target become exactly like source
func StreamDiff(ctx context.Context, table *Table, source RowStream, target RowStream, diffs chan Diff) error {
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

	var sourceRow *Row
	var targetRow *Row
	for {
		if advanceSource {
			sourceRow, err = source.Next()
			if err != nil {
				return errors.WithStack(err)
			}
			readsProcessed.WithLabelValues(table.Name, "source").Inc()
		}
		if advanceTarget {
			targetRow, err = target.Next()
			if err != nil {
				return errors.WithStack(err)
			}
			readsProcessed.WithLabelValues(table.Name, "target").Inc()
		}
		advanceSource = false
		advanceTarget = false

		if sourceRow != nil {
			if targetRow != nil {
				if sourceRow.ID < targetRow.ID {
					select {
					case diffs <- Diff{Insert, sourceRow, nil}:
					case <-ctx.Done():
						return nil
					}
					diffCount.WithLabelValues(table.Name, "insert").Inc()
					hasDiff = true
					advanceSource = true
					advanceTarget = false
				} else if sourceRow.ID > targetRow.ID {
					select {
					case diffs <- Diff{Delete, targetRow, nil}:
					case <-ctx.Done():
						return nil
					}
					diffCount.WithLabelValues(table.Name, "delete").Inc()
					hasDiff = true
					advanceSource = false
					advanceTarget = true
				} else {
					isEqual, err := RowsEqual(sourceRow, targetRow)
					if err != nil {
						return errors.WithStack(err)
					}
					if !isEqual {
						select {
						case diffs <- Diff{Update, sourceRow, targetRow}:
						case <-ctx.Done():
							return nil
						}
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
				select {
				case diffs <- Diff{Insert, sourceRow, nil}:
				case <-ctx.Done():
					return nil
				}
				diffCount.WithLabelValues(table.Name, "insert").Inc()
				hasDiff = true
				advanceSource = true
			}
		} else if targetRow != nil {
			hasDiff = true
			select {
			case diffs <- Diff{Delete, targetRow, nil}:
			case <-ctx.Done():
				return nil
			}
			diffCount.WithLabelValues(table.Name, "delete").Inc()
			hasDiff = true
			advanceTarget = true
		} else {
			return nil
		}
	}
}

func RowsEqual(sourceRow *Row, targetRow *Row) (bool, error) {
	for i := range sourceRow.Data {
		sourceValue := sourceRow.Data[i]
		targetValue := targetRow.Data[i]

		// Different database drivers interpret SQL types differently (it seems)
		sourceType := reflect.TypeOf(sourceValue)
		targetType := reflect.TypeOf(targetValue)
		if sourceType == targetType {
			// If they have the same type we just use reflect.DeepEqual and trust that
			if reflect.DeepEqual(sourceValue, targetValue) {
				continue
			} else {
				return false, nil
			}
		}

		if targetValue == nil {
			if sourceValue == nil {
				continue
			} else {
				return false, nil
			}
		}

		// If they do NOT have same type, we coerce the target type to the source type and then compare
		// We only support the combinations we've encountered in the wild here
		switch sourceValue := sourceValue.(type) {
		case nil:
			if targetValue != nil {
				return false, nil
			} else {
				continue
			}
		case int64:
			coerced, err := coerceInt64(targetValue)
			if err != nil {
				return false, errors.WithStack(err)
			}
			if sourceValue != coerced {
				return false, nil
			}
		case uint64:
			coerced, err := coerceUint64(targetValue)
			if err != nil {
				return false, errors.WithStack(err)
			}
			if sourceValue != coerced {
				return false, nil
			}
		case float64:
			coerced, err := coerceFloat64(targetValue)
			if err != nil {
				return false, errors.WithStack(err)
			}
			if sourceValue != coerced {
				return false, nil
			}
		default:
			return false, errors.Errorf("type combination %v -> %v not supported yet: source=%v target=%v",
				sourceType, targetType, sourceValue, targetValue)
		}
	}
	return true, nil
}

func coerceInt64(value interface{}) (int64, error) {
	switch value := value.(type) {
	case []byte:
		// This means it was sent as a unicode encoded string
		return strconv.ParseInt(string(value), 10, 64)
	default:
		return 0, nil
	}
}

func coerceUint64(value interface{}) (uint64, error) {
	switch value := value.(type) {
	case int64:
		return uint64(value), nil
	default:
		return 0, nil
	}
}

func coerceFloat64(value interface{}) (float64, error) {
	switch value := value.(type) {
	case float32:
		return float64(value), nil
	default:
		return 0, nil
	}
}

// readChunk reads a chunk without diffing producing only insert diffs
func readChunk(ctx context.Context, config ReaderConfig, source DBReader, chunk Chunk, diffs chan Diff) error {
	timer := prometheus.NewTimer(diffDuration.WithLabelValues(chunk.Table.Name))
	defer func() {
		timer.ObserveDuration()
		chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()
		rowsProcessed.WithLabelValues(chunk.Table.Name).Add(float64(chunk.Size))
	}()

	sourceStream, err := bufferChunk(ctx, config, source, "source", chunk)
	if err != nil {
		return errors.WithStack(err)
	}
	for {
		row, err := sourceStream.Next()
		if err != nil {
			return errors.WithStack(err)
		}
		if row == nil {
			break
		}
		readsProcessed.WithLabelValues(row.Table.Name, "source").Inc()
		diffs <- Diff{Insert, row, nil}
	}

	return nil
}

func diffChunk(ctx context.Context, config ReaderConfig, source DBReader, target DBReader, chunk Chunk, diffs chan Diff) error {
	timer := prometheus.NewTimer(diffDuration.WithLabelValues(chunk.Table.Name))
	defer func() {
		timer.ObserveDuration()
		chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()
		rowsProcessed.WithLabelValues(chunk.Table.Name).Add(float64(chunk.Size))
	}()

	if config.UseCRC32Checksum {
		// start off by running a fast checksum query
		sourceChecksum, err := checksumChunk(ctx, config, "source", source, chunk)
		if err != nil {
			return errors.WithStack(err)
		}
		targetChecksum, err := checksumChunk(ctx, config, "target", target, chunk)
		if err != nil {
			return errors.WithStack(err)
		}
		if sourceChecksum == targetChecksum {
			// Checksums match, no need to do any further diffing
			return nil
		}
	}

	sourceStream, err := bufferChunk(ctx, config, source, "source", chunk)
	if err != nil {
		return errors.WithStack(err)
	}
	targetStream, err := bufferChunk(ctx, config, target, "target", chunk)
	if err != nil {
		return errors.WithStack(err)
	}
	err = StreamDiff(ctx, chunk.Table, sourceStream, targetStream, diffs)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func checksumChunk(ctx context.Context, config ReaderConfig, from string, reader DBReader, chunk Chunk) (int64, error) {
	var checksum int64
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), config.ReadRetries), ctx)
	err := backoff.Retry(func() error {
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
		sql := fmt.Sprintf("SELECT %s BIT_XOR(%s) FROM `%s` %s",
			hint, strings.Join(chunk.Table.CRC32Columns, " ^ "), chunk.Table.Name, chunkWhere(chunk, extraWhereClause))
		rows, err := reader.QueryContext(ctx, sql)
		defer rows.Close()
		if err != nil {
			return errors.WithStack(err)
		}
		if !rows.Next() {
			return errors.Errorf("no checksum result")
		}
		err = rows.Scan(&checksum)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	}, b)

	return checksum, errors.WithStack(err)
}

// bufferChunk reads and buffers the chunk fully into memory so that we won't time out while diffing even if we have
// to pause due to back pressure from the writer
func bufferChunk(ctx context.Context, config ReaderConfig, source DBReader, from string, chunk Chunk) (RowStream, error) {
	var result RowStream

	logger := log.WithField("task", "differ").WithField("table", chunk.Table.Name)
	retriesLeft := config.ReadRetries
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), retriesLeft), ctx)
	err := backoff.RetryNotify(func() error {
		timer := prometheus.NewTimer(readDuration.WithLabelValues(chunk.Table.Name, from))
		defer timer.ObserveDuration()
		timeoutCtx, cancel := context.WithTimeout(ctx, config.ReadTimeout)
		defer cancel()
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
		stream, err := StreamChunk(timeoutCtx, source, chunk, hint, extraWhereClause)
		if err != nil {
			return errors.Wrapf(err, "failed to stream chunk from %s", from)
		}
		defer stream.Close()
		result, err = buffer(stream)
		if err != nil {
			return errors.Wrapf(err, "failed to stream chunk from %s", from)
		}
		return nil
	}, b, func(err error, duration time.Duration) {
		retriesLeft--
		logger.WithError(err).Warnf("failed reading chunk from %s, retrying in %s (retries left %d): %s",
			from, duration, retriesLeft, err)
	})

	return result, err
}
