package clone

import (
	"context"
	"database/sql"
	"reflect"
	"sync"

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
				return err
			}
			if sourceRow != nil {
				readsProcessed.WithLabelValues(sourceRow.Table.Name, "source").Inc()
			}
		}
		if advanceTarget {
			targetRow, err = target.Next(ctx)
			if err != nil {
				return err
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
				} else if !reflect.DeepEqual(sourceRow.Data, targetRow.Data) {
					diffs <- Diff{Update, sourceRow, targetRow}
					advanceSource = true
					advanceTarget = true
				} else {
					// Same!
					advanceSource = true
					advanceTarget = true
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

func DiffChunks(ctx context.Context, source *sql.Conn, target *sql.Conn, targetFilter []*topodata.KeyRange, chunks chan DiffRequest) error {
	for {
		select {
		case request, more := <-chunks:
			if !more {
				return nil
			}
			err := diffChunk(ctx, source, target, targetFilter, request)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			return nil
		}
	}
}

func diffChunk(ctx context.Context, source *sql.Conn, target *sql.Conn, targetFilter []*topodata.KeyRange, request DiffRequest) error {
	// TODO start off by running a fast checksum query

	chunk := request.Chunk
	diffs := request.Diffs

	sourceStream, err := StreamChunk(ctx, source, chunk)
	if err != nil {
		return errors.WithStack(err)
	}
	targetStream, err := StreamChunk(ctx, target, chunk)
	if len(targetFilter) > 0 {
		targetStream = filterStreamByShard(targetStream, chunk.Table, targetFilter)
	}
	if err != nil {
		return errors.WithStack(err)
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
