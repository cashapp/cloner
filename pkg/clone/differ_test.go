package clone

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testRow struct {
	id    int64
	table string
	data  string
}

func (testRow testRow) toRow() *Row {
	return &Row{
		Table: &Table{Name: testRow.table},
		ID:    testRow.id,
		Data:  []interface{}{testRow.data},
	}
}

type testDiff struct {
	diffType DiffType
	row      testRow
}

func (d testDiff) toDiff() Diff {
	return Diff{d.diffType, d.row.toRow(), nil}
}

func toTestDiff(diff Diff) testDiff {
	return testDiff{diff.Type, toTestRow(diff.Row)}
}

func toTestRow(row *Row) testRow {
	return testRow{row.ID, row.Table.Name, row.Data[0].(string)}
}

func TestStreamDiff(t *testing.T) {
	tests := []struct {
		name   string
		source []testRow
		target []testRow
		diff   []testDiff
	}{
		{
			name:   "empty",
			source: nil,
			target: nil,
			diff:   nil,
		},
		{
			name: "same",
			source: []testRow{
				{id: 1, data: "A"},
			},
			target: []testRow{
				{id: 1, data: "A"},
			},
			diff: nil,
		},
		{
			name: "1 new",
			source: []testRow{
				{id: 1, data: "A"},
			},
			target: nil,
			diff:   []testDiff{{Insert, testRow{id: 1, data: "A"}}},
		},
		{
			name:   "1 deleted",
			source: nil,
			target: []testRow{
				{id: 1, data: "A"},
			},
			diff: []testDiff{{Delete, testRow{id: 1, data: "A"}}},
		},
		{
			name: "1 updated",
			source: []testRow{
				{id: 1, data: "B"},
			},
			target: []testRow{
				{id: 1, data: "A"},
			},
			diff: []testDiff{{Update, testRow{id: 1, data: "B"}}},
		},
		{
			name: "1 same 1 inserted",
			source: []testRow{
				{id: 1, data: "A"},
				{id: 2, data: "B"},
			},
			target: []testRow{
				{id: 1, data: "A"},
			},
			diff: []testDiff{{Insert, testRow{id: 2, data: "B"}}},
		},
		{
			name: "1 same 1 deleted",
			source: []testRow{
				{id: 1, data: "A"},
			},
			target: []testRow{
				{id: 1, data: "A"},
				{id: 2, data: "B"},
			},
			diff: []testDiff{{Delete, testRow{id: 2, data: "B"}}},
		},
		{
			name: "1 same 1 updated",
			source: []testRow{
				{id: 1, data: "A"},
				{id: 2, data: "B"},
			},
			target: []testRow{
				{id: 1, data: "A"},
				{id: 2, data: "A"},
			},
			diff: []testDiff{{Update, testRow{id: 2, data: "B"}}},
		},
		{
			name: "1 inserted middle",
			source: []testRow{
				{id: 1, data: "A"},
				{id: 2, data: "B"},
				{id: 3, data: "C"},
			},
			target: []testRow{
				{id: 1, data: "A"},
				{id: 3, data: "C"},
			},
			diff: []testDiff{{Insert, testRow{id: 2, data: "B"}}},
		},
		{
			name: "2 inserted middle",
			source: []testRow{
				{id: 1, data: "A"},
				{id: 2, data: "B"},
				{id: 3, data: "C"},
				{id: 4, data: "D"},
			},
			target: []testRow{
				{id: 1, data: "A"},
				{id: 4, data: "D"},
			},
			diff: []testDiff{
				{Insert, testRow{id: 2, data: "B"}},
				{Insert, testRow{id: 3, data: "C"}},
			},
		},
		{
			name: "2 deleted middle",
			source: []testRow{
				{id: 1, data: "A"},
				{id: 4, data: "D"},
			},
			target: []testRow{
				{id: 1, data: "A"},
				{id: 2, data: "B"},
				{id: 3, data: "C"},
				{id: 4, data: "D"},
			},
			diff: []testDiff{
				{Delete, testRow{id: 2, data: "B"}},
				{Delete, testRow{id: 3, data: "C"}},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			diffsChan := make(chan Diff)
			var result []testDiff
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for diff := range diffsChan {
					result = append(result, toTestDiff(diff))
				}
			}()
			err := StreamDiff(context.Background(), streamTestRows(test.source), streamTestRows(test.target), diffsChan)
			assert.NoError(t, err)
			close(diffsChan)
			wg.Wait()
			assert.Equal(t, test.diff, result)
		})
	}
}

type testRowStreamer struct {
	rows []testRow
}

func (t *testRowStreamer) Next(context.Context) (*Row, error) {
	if len(t.rows) == 0 {
		return nil, nil
	}
	testRow := t.rows[0]
	// chop head
	t.rows = t.rows[1:]
	return testRow.toRow(), nil
}

func streamTestRows(rows []testRow) RowStream {
	return &testRowStreamer{rows}
}

func TestRowsEqual(t *testing.T) {
	sourceRow := &Row{nil, 0, 0, []interface{}{
		100020406,
		int64(30027935561),
		[]byte{51, 48, 48, 50, 55, 57, 51, 53, 53, 54, 49},
	}}
	targetRow := &Row{nil, 0, 0, []interface{}{
		100020406,
		[]byte{51, 48, 48, 50, 55, 57, 51, 53, 53, 54, 49},
		[]byte{51, 48, 48, 50, 55, 57, 51, 53, 53, 54, 49},
	}}
	isEqual, err := RowsEqual(
		sourceRow,
		targetRow,
	)
	assert.NoError(t, err)
	assert.True(t, isEqual)
}
