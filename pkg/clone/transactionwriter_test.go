package clone

import (
	mysqlschema "github.com/go-mysql-org/go-mysql/schema"
	"github.com/stretchr/testify/require"
	"testing"
)

func TestTransactionSetAppend(t *testing.T) {
	table := &Table{Name: "customers", MysqlTable: &mysqlschema.Table{
		PKColumns: []int{0},
		Columns:   []mysqlschema.TableColumn{{Name: "id"}, {Name: "name"}},
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
							Start: 0,
							End:   10,
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
								Start: 0,
								End:   10,
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
							Start: 0,
							End:   10,
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
								Start: 0,
								End:   10,
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
							Start: 0,
							End:   10,
						},
					}},
				},
				{
					Mutations: []Mutation{{
						Type:  Repair,
						Table: table,
						Chunk: Chunk{
							Table: table,
							Start: 10,
							End:   20,
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
								Start: 0,
								End:   10,
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
								Start: 10,
								End:   20,
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
							Start: 0,
							End:   10,
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
							Start: 10,
							End:   20,
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
								Start: 0,
								End:   10,
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
								Start: 10,
								End:   20,
							},
						}},
					},
				},
			},
		},
		// TODO test cases with chunk repairs
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			transactionSet := transactionSet{}
			for _, transaction := range test.input {
				transactionSet.Append(transaction)
			}
			require.Equal(t, len(test.output), len(transactionSet.sequences))
			for i, sequence := range test.output {
				require.Equal(t, sequence, transactionSet.sequences[i].transactions)
			}
		})
	}
}
