package clone

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSplitBatch(t *testing.T) {
	b1, b2 := splitBatch(Batch{
		Type:  Insert,
		Table: nil,
		Rows: []*Row{
			{
				Data: []interface{}{0},
			},
			{
				Data: []interface{}{1},
			},
			{
				Data: []interface{}{2},
			},
			{
				Data: []interface{}{3},
			},
			{
				Data: []interface{}{4},
			},
		},
	})
	assert.Equal(t, b1.Rows, []*Row{
		{
			Data: []interface{}{0},
		},
		{
			Data: []interface{}{1},
		},
	})
	assert.Equal(t, b2.Rows, []*Row{
		{
			Data: []interface{}{2},
		},
		{
			Data: []interface{}{3},
		},
		{
			Data: []interface{}{4},
		},
	})
}
