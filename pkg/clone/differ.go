package clone

import (
	"context"
	"reflect"
	"strconv"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
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

type DiffRequest struct {
	// Chunk to diff
	Chunk Chunk
	// Channel to send the diffs to
	Diffs chan Diff
	// Use this to signal we're done
	Done *sync.WaitGroup
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
		switch sourceValue.(type) {
		case nil:
			if targetValue == nil {
				continue
			}
		case int64:
			coerced, err := coerceInt64(targetValue)
			if err != nil {
				return false, errors.WithStack(err)
			}
			if sourceValue.(int64) != coerced {
				return false, nil
			}
		case uint64:
			coerced, err := coerceUint64(targetValue)
			if err != nil {
				return false, errors.WithStack(err)
			}
			if sourceValue.(uint64) != coerced {
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
	switch value.(type) {
	case []byte:
		// This means it was sent as a unicode encoded string
		return strconv.ParseInt(string(value.([]byte)), 10, 64)
	default:
		return 0, nil
	}
}

func coerceUint64(value interface{}) (uint64, error) {
	switch value.(type) {
	case int64:
		return uint64(value.(int64)), nil
	default:
		return 0, nil
	}
}

func DiffChunks(ctx context.Context, source DBReader, target DBReader, targetFilter []*topodata.KeyRange, timeout time.Duration, chunks chan DiffRequest) error {
	for {
		select {
		case request, more := <-chunks:
			if !more {
				return nil
			}
			err := diffChunk(ctx, source, target, targetFilter, request, timeout)
			if err != nil {
				return errors.WithStack(err)
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func diffChunk(ctx context.Context, source DBReader, target DBReader, targetFilter []*topodata.KeyRange, request DiffRequest, timeout time.Duration) error {
	// TODO start off by running a fast checksum query
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	chunk := request.Chunk
	diffs := request.Diffs

	sourceStream, err := StreamChunk(ctx, source, chunk)
	if err != nil {
		return errors.Wrapf(err, "failed to stream chunk from source")
	}
	targetStream, err := StreamChunk(ctx, target, chunk)
	if err != nil {
		return errors.Wrapf(err, "failed to stream chunk from target")
	}
	if len(targetFilter) > 0 {
		targetStream = filterStreamByShard(targetStream, chunk.Table, targetFilter)
	}
	err = StreamDiff(ctx, sourceStream, targetStream, diffs)
	if err != nil {
		return errors.WithStack(err)
	}
	chunksProcessed.WithLabelValues(chunk.Table.Name).Inc()
	// Signal we're done to the requester
	request.Done.Done()
	return nil
}
