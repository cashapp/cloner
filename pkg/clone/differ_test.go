package clone

import (
	"context"
	"github.com/alecthomas/kong"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
)

type testRow struct {
	id    int64
	table string
	data  string
}

func (testRow testRow) toRow(batchSize int) *Row {
	return &Row{
		Table: &Table{Name: testRow.table, Config: TableConfig{WriteBatchSize: batchSize}},
		ID:    testRow.id,
		Data:  []interface{}{testRow.data},
	}
}

type testDiff struct {
	diffType DiffType
	row      testRow
}

func (d testDiff) toDiff(batchSize int) Diff {
	return Diff{d.diffType, d.row.toRow(batchSize), nil}
}

func toTestDiff(diff Diff) testDiff {
	return testDiff{diff.Type, toTestRow(diff.Row)}
}

func toTestRow(row *Row) testRow {
	return testRow{row.ID, row.Table.Name, row.Data[0].(string)}
}

func TestStreamDiff(t *testing.T) {
	table := &Table{Name: "foobar"}
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
			err := StreamDiff(context.Background(), table,
				streamTestRows(test.source, 5),
				streamTestRows(test.target, 5),
				diffsChan)
			assert.NoError(t, err)
			close(diffsChan)
			wg.Wait()
			assert.Equal(t, test.diff, result)
		})
	}
}

type testRowStreamer struct {
	rows      []testRow
	batchSize int
}

func (t *testRowStreamer) Close() error {
	return nil
}

func (t *testRowStreamer) Next() (*Row, error) {
	if len(t.rows) == 0 {
		return nil, nil
	}
	testRow := t.rows[0]
	// chop head
	t.rows = t.rows[1:]
	return testRow.toRow(t.batchSize), nil
}

func streamTestRows(rows []testRow, batchSize int) RowStream {
	return &testRowStreamer{rows, batchSize}
}

func TestRowsEqual(t *testing.T) {
	sourceRow := &Row{nil, 0, []interface{}{
		100020406,
		int64(30027935561),
		[]byte{51, 48, 48, 50, 55, 57, 51, 53, 53, 54, 49},
	}}
	targetRow := &Row{nil, 0, []interface{}{
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

func TestDiffWithChecksum(t *testing.T) {
	err := startAll()
	assert.NoError(t, err)

	source := vitessContainer.Config()
	source.Database = "customer/-80@replica"
	target := tidbContainer.Config()

	config := &ReaderConfig{
		SourceTargetConfig: SourceTargetConfig{
			Source: source,
			Target: target,
		},
	}
	err = kong.ApplyDefaults(config)
	config.UseCRC32Checksum = true

	tables, err := LoadTables(context.Background(), *config)
	assert.NoError(t, err)

	sourceReader, err := config.Source.DB()
	assert.NoError(t, err)
	defer sourceReader.Close()
	targetReader, err := config.Target.DB()
	assert.NoError(t, err)
	defer targetReader.Close()
	r := NewReader(*config, tables[0], sourceReader, nil, targetReader, nil)

	type row struct {
		id   int64
		name string
	}
	type diff struct {
		diffType DiffType
		row      row
	}
	tests := []struct {
		name   string
		source []row
		target []row
		diff   []diff
	}{
		{
			name:   "empty",
			source: nil,
			target: nil,
			diff:   nil,
		},
		// TODO add more tests
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err = deleteAllData(source)
			assert.NoError(t, err)
			err = deleteAllData(target)
			assert.NoError(t, err)

			// TODO insert data, how can I make sure they end up in -80?

			diffsChan := make(chan Diff)
			var result []diff
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for d := range diffsChan {
					result = append(result, diff{d.Type, row{d.Row.ID, d.Row.Data[0].(string)}})
				}
			}()

			ctx := context.Background()
			chunk := Chunk{
				Table: tables[0],
			}
			err = r.diffChunk(ctx, chunk, diffsChan)
			assert.NoError(t, err)
			close(diffsChan)
			wg.Wait()
			assert.Equal(t, test.diff, result)
		})
	}
}
