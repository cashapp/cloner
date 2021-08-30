package clone

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testBatch struct {
	diffType DiffType
	table    string
	rows     []testRow
}

func toTestBatch(batch Batch) testBatch {
	var rows []testRow
	for _, row := range batch.Rows {
		rows = append(rows, toTestRow(row))
	}
	return testBatch{batch.Type, batch.Table.Name, rows}
}

func TestBatcher(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int
		diffs     []testDiff
		batches   []testBatch
	}{
		{
			name:      "empty",
			batchSize: 1,
			diffs:     nil,
			batches:   nil,
		},
		{
			name:      "one",
			batchSize: 1,
			diffs:     []testDiff{{Insert, testRow{1, "t1", "A"}}},
			batches: []testBatch{
				{diffType: Insert, table: "t1",
					rows: []testRow{testRow{1, "t1", "A"}}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diffChan := make(chan Diff, len(test.diffs))
			for _, diff := range test.diffs {
				diffChan <- diff.toDiff(test.batchSize)
			}
			close(diffChan)
			var result []testBatch
			batchChan := make(chan Batch)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for batch := range batchChan {
					result = append(result, toTestBatch(batch))
				}
			}()
			err := BatchWrites(context.Background(), diffChan, batchChan)
			assert.NoError(t, err)
			wg.Wait()
			assert.Equal(t, test.batches, result)
		})
	}
}

func TestBatchTableWrites2(t *testing.T) {
	tests := []struct {
		name      string
		batchSize int
		diffs     []testDiff
		batches   []testBatch
	}{
		{
			name:      "empty",
			batchSize: 1,
			diffs:     nil,
			batches:   nil,
		},
		{
			name:      "one",
			batchSize: 1,
			diffs:     []testDiff{{Insert, testRow{1, "t1", "A"}}},
			batches: []testBatch{
				{diffType: Insert, table: "t1",
					rows: []testRow{testRow{1, "t1", "A"}}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diffs := make([]Diff, len(test.diffs))
			for _, diff := range test.diffs {
				diffs = append(diffs, diff.toDiff(test.batchSize))
			}
			var result []testBatch
			batches, err := BatchTableWritesSync(diffs)
			assert.NoError(t, err)
			for _, batch := range batches {
				result = append(result, toTestBatch(batch))
			}
			assert.Equal(t, test.batches, result)
		})
	}
}
