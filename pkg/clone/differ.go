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
	readsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "reads_processed",
			Help: "How many rows read by table.",
		},
		[]string{"table", "side"},
	)
	chunksProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chunks_processed",
			Help: "How many chunks has been processed, partitioned by table.",
		},
		[]string{"table"},
	)
)

func init() {
	prometheus.MustRegister(readsProcessed)
	prometheus.MustRegister(chunksEnqueued)
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
	advanceSource := true
	advanceTarget := true

	var err error
	var sourceRow *Row
	var targetRow *Row
	for {
		if advanceSource {
			sourceRow, err = source.Next(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			if sourceRow != nil {
				readsProcessed.WithLabelValues(sourceRow.Table.Name, "source").Inc()
			}
		}
		if advanceTarget {
			targetRow, err = target.Next(ctx)
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
					diffs <- Diff{Insert, sourceRow, nil}
					advanceSource = true
					advanceTarget = false
				} else if sourceRow.ID > targetRow.ID {
					diffs <- Diff{Delete, targetRow, nil}
					advanceSource = false
					advanceTarget = true
				} else {
					isEqual, err := RowsEqual(sourceRow, targetRow)
					if err != nil {
						return errors.WithStack(err)
					}
					if !isEqual {
						diffs <- Diff{Update, sourceRow, targetRow}
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

func diffChunk(ctx context.Context, config ReaderConfig, source DBReader, target DBReader, targetFilter []*topodata.KeyRange, chunk Chunk, diffs chan Diff) error {
	logger := log.WithField("task", "differ").WithField("table", chunk.Table.Name)

	retriesLeft := config.ReadRetries
	b := backoff.WithContext(backoff.WithMaxRetries(backoff.NewExponentialBackOff(), retriesLeft), ctx)
	err := backoff.RetryNotify(func() error {
		// TODO start off by running a fast checksum query

		ctx, cancel := context.WithTimeout(ctx, config.ReadTimeout)
		defer cancel()

		sourceStream, err := StreamChunk(ctx, source, chunk)
		if err != nil {
			return errors.Wrapf(err, "failed to stream chunk from source")
		}
		defer sourceStream.Close()
		targetStream, err := StreamChunk(ctx, target, chunk)
		if err != nil {
			logger.WithError(err).Warnf("failed to stream chunk from target")
			return errors.Wrapf(err, "failed to stream chunk from target")
		}
		defer targetStream.Close()
		if len(targetFilter) > 0 {
			targetStream = filterStreamByShard(targetStream, chunk.Table, targetFilter)
		}
		err = StreamDiff(ctx, sourceStream, targetStream, diffs)
		if err != nil {
			logger.WithError(err).Warnf("failed to diff chunk")
			return errors.WithStack(err)
		}

		return nil
	}, b, func(err error, duration time.Duration) {
		retriesLeft--
		logger.WithError(err).Warnf("failed diffing, retrying in %s (retries left %d): %s",
			duration, retriesLeft, err)
	})

	if err != nil {
		logger.Error(err)
		return errors.WithStack(err)
	}

	chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()
	return nil
}
