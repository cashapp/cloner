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
				ID: 0,
			},
			{
				ID: 1,
			},
			{
				ID: 2,
			},
			{
				ID: 3,
			},
			{
				ID: 4,
			},
		},
	})
	assert.Equal(t, b1.Rows, []*Row{
		{
			ID: 0,
		},
		{
			ID: 1,
		},
	})
	assert.Equal(t, b2.Rows, []*Row{
		{
			ID: 2,
		},
		{
			ID: 3,
		},
		{
			ID: 4,
		},
	})
}
