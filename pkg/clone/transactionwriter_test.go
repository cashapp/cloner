package clone

import (
	"context"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"

	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/require"
)

func TestTransactionSetAppend(t *testing.T) {
	table := &Table{Name: "customers",
		KeyColumns:       []string{"id"},
		KeyColumnIndexes: []int{0},
		MysqlTable: &mysqlschema.Table{
			PKColumns: []int{0},
			Columns:   []mysqlschema.TableColumn{{Name: "id"}, {Name: "name"}},
		}}
	multiKeyTable := &Table{Name: "transaction",
		KeyColumns:       []string{"customer_id", "id"},
		KeyColumnIndexes: []int{1, 0},
		MysqlTable: &mysqlschema.Table{
			PKColumns: []int{1, 0},
			Columns:   []mysqlschema.TableColumn{{Name: "id"}, {Name: "customer_id"}, {Name: "amount_cents"}},
		}}
	tests := []struct {
		name   string
		input  []Transaction
		output [][]Transaction
	}{
		{
			name:   "no transactions",
			input:  []Transaction{},
			output: [][]Transaction{},
		},
		{
			name: "one transaction",
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: table,
						Rows: [][]interface{}{
							{1, "Customer #1"},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: table,
							Rows: [][]interface{}{
								{1, "Customer #1"},
							},
						}},
						FinalPosition: Position{},
					},
				},
			},
		},
		{
			name: "two causal transactions",
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: table,
						Rows: [][]interface{}{
							{1, "Customer #1"},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Update,
						Table: table,
						Rows: [][]interface{}{
							{1, "Updated Customer #1"},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: table,
							Rows: [][]interface{}{
								{1, "Customer #1"},
							},
						}},
					},
					{
						Mutations: []Mutation{{
							Type:  Update,
							Table: table,
							Rows: [][]interface{}{
								{1, "Updated Customer #1"},
							},
						}},
					},
				},
			},
		},
		{
			name: "two non-causal transactions",
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: table,
						Rows: [][]interface{}{
							{1, "Customer #1"},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: table,
						Rows: [][]interface{}{
							{2, "Customer #2"},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: table,
							Rows: [][]interface{}{
								{1, "Customer #1"},
							},
						}},
					},
				},
				{
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: table,
							Rows: [][]interface{}{
								{2, "Customer #2"},
							},
						}},
					},
				},
			},
		},
		{
			name: "one chunk and one transaction inside the chunk",
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: table,
						Chunk: Chunk{
							Table: table,
							Start: []interface{}{0},
							End:   []interface{}{10},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: table,
						Rows: [][]interface{}{
							{2, "Customer #2"},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: table,
							Chunk: Chunk{
								Table: table,
								Start: []interface{}{0},
								End:   []interface{}{10},
							},
						}},
					},
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: table,
							Rows: [][]interface{}{
								{2, "Customer #2"},
							},
						}},
					},
				},
			},
		},
		{
			name: "one chunk and one transaction outside the chunk",
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: table,
						Chunk: Chunk{
							Table: table,
							Start: []interface{}{0},
							End:   []interface{}{10},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: table,
						Rows: [][]interface{}{
							{11, "Customer #11"},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: table,
							Chunk: Chunk{
								Table: table,
								Start: []interface{}{0},
								End:   []interface{}{10},
							},
						}},
					},
				},
				{
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: table,
							Rows: [][]interface{}{
								{11, "Customer #11"},
							},
						}},
					},
				},
			},
		},
		{
			name: "two chunks",
			// chunks can in general apply in parallel since chunks never overlap
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: table,
						Chunk: Chunk{
							Table: table,
							Start: []interface{}{0},
							End:   []interface{}{10},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: table,
						Chunk: Chunk{
							Table: table,
							Start: []interface{}{10},
							End:   []interface{}{20},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: table,
							Chunk: Chunk{
								Table: table,
								Start: []interface{}{0},
								End:   []interface{}{10},
							},
						}},
					},
				},
				{
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: table,
							Chunk: Chunk{
								Table: table,
								Start: []interface{}{10},
								End:   []interface{}{20},
							},
						}},
					},
				},
			},
		},
		{
			name: "two chunks and a spanning transaction",
			// this test shows two chunks and a transaction in between that has rows from both chunks
			// this forces the chunks to apply sequentially even if chunks usually can apply in parallel
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: table,
						Chunk: Chunk{
							Table: table,
							Start: []interface{}{0},
							End:   []interface{}{10},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: table,
						Rows: [][]interface{}{
							{5, "Customer #5"},
							{15, "Customer #15"},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: table,
						Chunk: Chunk{
							Table: table,
							Start: []interface{}{10},
							End:   []interface{}{20},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: table,
							Chunk: Chunk{
								Table: table,
								Start: []interface{}{0},
								End:   []interface{}{10},
							},
						}},
					},
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: table,
							Rows: [][]interface{}{
								{5, "Customer #5"},
								{15, "Customer #15"},
							},
						}},
					},
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: table,
							Chunk: Chunk{
								Table: table,
								Start: []interface{}{10},
								End:   []interface{}{20},
							},
						}},
					},
				},
			},
		},
		{
			name: "multi keys one chunk and one transaction outside the chunk",
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: multiKeyTable,
						Chunk: Chunk{
							Table: multiKeyTable,
							Start: []interface{}{0, 0},
							End:   []interface{}{10, 0},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: multiKeyTable,
						Rows: [][]interface{}{
							{0, 11, "11"},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: multiKeyTable,
							Chunk: Chunk{
								Table: multiKeyTable,
								Start: []interface{}{0, 0},
								End:   []interface{}{10, 0},
							},
						}},
					},
				},
				{
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: multiKeyTable,
							Rows: [][]interface{}{
								{0, 11, "11"},
							},
						}},
					},
				},
			},
		},
		{
			name: "multi keys two chunks",
			// chunks can in general apply in parallel since chunks never overlap
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: multiKeyTable,
						Chunk: Chunk{
							Table: multiKeyTable,
							Start: []interface{}{0, 0},
							End:   []interface{}{10, 0},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: multiKeyTable,
						Chunk: Chunk{
							Table: multiKeyTable,
							Start: []interface{}{10, 0},
							End:   []interface{}{20, 0},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: multiKeyTable,
							Chunk: Chunk{
								Table: multiKeyTable,
								Start: []interface{}{0, 0},
								End:   []interface{}{10, 0},
							},
						}},
					},
				},
				{
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: multiKeyTable,
							Chunk: Chunk{
								Table: multiKeyTable,
								Start: []interface{}{10, 0},
								End:   []interface{}{20, 0},
							},
						}},
					},
				},
			},
		},
		{
			name: "multi key two chunks and a spanning transaction",
			// this test shows two chunks and a transaction in between that has rows from both chunks
			// this forces the chunks to apply sequentially even if chunks usually can apply in parallel
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: multiKeyTable,
						Chunk: Chunk{
							Table: multiKeyTable,
							Start: []interface{}{0, 0},
							End:   []interface{}{10, 0},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: multiKeyTable,
						Rows: [][]interface{}{
							{1, 5, 500},
							{2, 15, 1500},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: multiKeyTable,
						Chunk: Chunk{
							Table: multiKeyTable,
							Start: []interface{}{10, 0},
							End:   []interface{}{20, 0},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: multiKeyTable,
							Chunk: Chunk{
								Table: multiKeyTable,
								Start: []interface{}{0, 0},
								End:   []interface{}{10, 0},
							},
						}},
					},
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: multiKeyTable,
							Rows: [][]interface{}{
								{1, 5, 500},
								{2, 15, 1500},
							},
						}},
					},
					{
						Mutations: []Mutation{{
							Type:  Repair,
							Table: multiKeyTable,
							Chunk: Chunk{
								Table: multiKeyTable,
								Start: []interface{}{10, 0},
								End:   []interface{}{20, 0},
							},
						}},
					},
				},
			},
		},
		{
			name: "two non-causal transactions then a spanning transaction",
			input: []Transaction{
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: table,
						Rows: [][]interface{}{
							{1, "Customer #1"},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Update,
						Table: table,
						Rows: [][]interface{}{
							{1, "Updated Customer #1"},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Insert,
						Table: table,
						Rows: [][]interface{}{
							{2, "Customer #2"},
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Update,
						Table: table,
						Rows: [][]interface{}{
							{1, "Updated Customer #1"},
							{2, "Updated Customer #2"},
						},
					}},
				},
			},
			output: [][]Transaction{
				{
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: table,
							Rows: [][]interface{}{
								{1, "Customer #1"},
							},
						}},
					},
					{
						Mutations: []Mutation{{
							Type:  Update,
							Table: table,
							Rows: [][]interface{}{
								{1, "Updated Customer #1"},
							},
						}},
					},
					{
						Mutations: []Mutation{{
							Type:  Insert,
							Table: table,
							Rows: [][]interface{}{
								{2, "Customer #2"},
							},
						}},
					},
					{
						Mutations: []Mutation{{
							Type:  Update,
							Table: table,
							Rows: [][]interface{}{
								{1, "Updated Customer #1"},
								{2, "Updated Customer #2"},
							},
						}},
					},
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transactionSet := transactionSet{}
			for _, transaction := range test.input {
				transactionSet.Append(transaction)
			}
			require.Equal(t, len(test.output), len(transactionSet.sequences))
			for i, sequence := range test.output {
				var actualTransactions []Transaction
				for _, t := range transactionSet.sequences[i].transactions {
					actualTransactions = append(actualTransactions, t.transaction)
				}
				require.Equal(t, sequence, actualTransactions)
			}
		})
	}
}

func TestIgnoredColumnsReplaceStatement(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	table := &Table{
		Name: "mytable",
		MysqlTable: &mysqlschema.Table{
			Name: "mytable",
			Columns: []mysqlschema.TableColumn{
				{Name: "mycolumn1"},
				{Name: "mycolumn2"},
				{Name: "mycolumn3"},
				{Name: "mycolumn4"},
			},
		},
	}
	mutation := Mutation{
		Type:  Update,
		Table: table,
		Rows: [][]interface{}{
			{"value11", "value12", "value13", "value14"},
			{"value21", "value22", "value23", "value24"},
		},
		Chunk: Chunk{},
	}

	// First test without ignoring columns
	config := ReaderConfig{}
	table.IgnoredColumnsBitmap = ignoredColumnsBitmap(config, table.MysqlTable)
	writer := NewMockDBWriter(ctrl)
	writer.EXPECT().ExecContext(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(ctx context.Context, query string, args ...string) {
		assert.Equal(t,
			"REPLACE INTO mytable (`mycolumn1`,`mycolumn2`,`mycolumn3`,`mycolumn4`) VALUES (?,?,?,?),(?,?,?,?)",
			query)
		assert.Equal(t, args, []string{
			"value11", "value12", "value13", "value14",
			"value21", "value22", "value23", "value24",
		})
	})
	err := mutation.replace(context.Background(), writer)
	assert.NoError(t, err)

	// Then test with ignored columns
	config = ReaderConfig{
		SourceTargetConfig: SourceTargetConfig{
			IgnoreColumns: []string{"mytable.mycolumn2", "mytable.mycolumn4"},
		},
	}
	table.IgnoredColumnsBitmap = ignoredColumnsBitmap(config, table.MysqlTable)
	writer = NewMockDBWriter(ctrl)
	writer.EXPECT().ExecContext(gomock.Any(), gomock.Any(), gomock.Any()).Do(func(ctx context.Context, query string, args ...string) {
		assert.Equal(t,
			"REPLACE INTO mytable (`mycolumn1`,`mycolumn3`) VALUES (?,?),(?,?)",
			query)
		assert.Equal(t, args, []string{
			"value11", "value13",
			"value21", "value23",
		})
	})
	err = mutation.replace(context.Background(), writer)
	assert.NoError(t, err)
}
