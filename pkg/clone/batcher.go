package clone

import (
	"context"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	writesEnqueued = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "writes_enqueued",
			Help: "How many writes, partitioned by table and type (insert, update, delete).",
		},
		[]string{"table", "type"},
	)
)

func init() {
	prometheus.MustRegister(writesEnqueued)
}

type Batch struct {
	Type      DiffType
	Table     *Table
	Rows      []*Row
	Retries   int
	LastError error
}

// BatchWrites consumes diffs and batches them up into batches by type and table
func BatchWrites(ctx context.Context, batchSize int, diffs chan Diff, batches chan Batch) error {
	batchesByType := make(map[DiffType]map[string]Batch)

readChannel:
	for {
		select {
		case diff, more := <-diffs:
			if more {
				batchesByTable, ok := batchesByType[diff.Type]
				if !ok {
					batchesByTable = make(map[string]Batch)
					batchesByType[diff.Type] = batchesByTable
				}
				batch, ok := batchesByTable[diff.Row.Table.Name]
				if !ok {
					batch = Batch{diff.Type, diff.Row.Table, nil, 0, nil}
				}
				batch.Rows = append(batch.Rows, diff.Row)

				if len(batch.Rows) >= batchSize {
					// Batch is full send it
					requestWrite(batches, nil, batch)
					// and clear it
					batch.Rows = nil
				}

				batchesByTable[diff.Row.Table.Name] = batch
			} else {
				break readChannel
			}
		case <-ctx.Done():
			break readChannel
		}
	}

	// Write the final unfilled batches
	for _, batchesByTable := range batchesByType {
		for _, batch := range batchesByTable {
			if len(batch.Rows) > 0 {
				requestWrite(batches, nil, batch)
			}
		}
	}
	close(batches)

	return nil
}

// BatchTableWrites consumes diffs for a single table and batches them up into batches by type
func BatchTableWrites(ctx context.Context, batchSize int, diffs chan Diff, writeRequests chan Batch, writesInFlight *sync.WaitGroup) error {
	batchesByType := make(map[DiffType]Batch)

	for {
		select {
		case diff, more := <-diffs:
			if !more {
				// Write the final unfilled batches
				for _, batch := range batchesByType {
					requestWrite(writeRequests, writesInFlight, batch)
				}
				return nil
			}

			batch, ok := batchesByType[diff.Type]
			if !ok {
				batch = Batch{diff.Type, diff.Row.Table, nil, 0, nil}
			}
			batch.Rows = append(batch.Rows, diff.Row)

			if len(batch.Rows) >= batchSize {
				// Batch is full, send it
				requestWrite(writeRequests, writesInFlight, batch)
				// and clear it
				batch.Rows = nil
			}

			batchesByType[diff.Type] = batch
		case <-ctx.Done():
			return nil
		}
	}
}

func requestWrite(writeRequests chan Batch, writesInFlight *sync.WaitGroup, batch Batch) {
	if len(batch.Rows) > 0 {
		writesEnqueued.WithLabelValues(batch.Table.Name, string(batch.Type)).Add(float64(len(batch.Rows)))
		writesInFlight.Add(1)
		writeRequests <- batch
	}
}
