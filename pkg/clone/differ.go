package clone

import (
	"context"
	"reflect"
	"strconv"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/proto/topodata"
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
	readsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reads_processed",
			Help: "How many rows read by table.",
		},
		[]string{"table", "side"},
	)
	readDuration = prometheus.NewSummaryVec(
		prometheus.SummaryOpts{
			Name:       "read_duration",
			Help:       "Total duration of the database read (including retries and backoff) of a chunk from a table from either source or target.",
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
	readLimiterDelay = prometheus.NewSummary(
		prometheus.SummaryOpts{
			Name: "read_limiter_delay_duration",
			Help: "Duration of back off from the concurrency limiter for reads.",
		},
	)
)

func init() {
	prometheus.MustRegister(readsProcessed)
	prometheus.MustRegister(chunksEnqueued)
	prometheus.MustRegister(chunksProcessed)
	prometheus.MustRegister(readDuration)
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
func StreamDiff(ctx context.Context, source RowStream, target RowStream, diffs chan Diff) error {
	var err error

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
			if sourceRow != nil {
				readsProcessed.WithLabelValues(sourceRow.Table.Name, "source").Inc()
			}
		}
		if advanceTarget {
			targetRow, err = target.Next()
			if err != nil {
				return errors.WithStack(err)
			}
			if targetRow != nil {
				readsProcessed.WithLabelValues(targetRow.Table.Name, "target").Inc()
			}
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
					advanceSource = true
					advanceTarget = false
				} else if sourceRow.ID > targetRow.ID {
					select {
					case diffs <- Diff{Delete, targetRow, nil}:
					case <-ctx.Done():
						return nil
					}
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
						advanceSource = true
						advanceTarget = true
					} else {
						// Same!
						advanceSource = true
						advanceTarget = true
					}
				}
			} else {
				diffs <- Diff{Insert, sourceRow, nil}
				advanceSource = true
			}
		} else if targetRow != nil {
			diffs <- Diff{Delete, targetRow, nil}
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
	defer timer.ObserveDuration()

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

	chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()

	return nil
}

func diffChunk(ctx context.Context, config ReaderConfig, source DBReader, target DBReader, targetFilter []*topodata.KeyRange, chunk Chunk, diffs chan Diff) error {
	timer := prometheus.NewTimer(diffDuration.WithLabelValues(chunk.Table.Name))
	defer timer.ObserveDuration()

	// TODO start off by running fast checksum queries

	sourceStream, err := bufferChunk(ctx, config, source, "source", chunk)
	if err != nil {
		return errors.WithStack(err)
	}
	targetStream, err := bufferChunk(ctx, config, target, "target", chunk)
	if err != nil {
		return errors.WithStack(err)
	}
	err = StreamDiff(ctx, sourceStream, targetStream, diffs)
	if err != nil {
		return errors.WithStack(err)
	}

	chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()

	return nil
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
		if from == "target" {
			extraWhereClause = chunk.Table.Config.TargetWhere
		}
		if from == "source" {
			extraWhereClause = chunk.Table.Config.SourceWhere
		}
		stream, err := StreamChunk(timeoutCtx, source, chunk, extraWhereClause)
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
