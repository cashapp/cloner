package clone

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestExpandRowConstructorComparison(t *testing.T) {
	tests := []struct {
		name      string
		left      []string
		operator  string
		right     []interface{}
		expansion string
		params    []interface{}
	}{
		{
			name:      "one operand >",
			left:      []string{"id"},
			operator:  ">",
			right:     []interface{}{1000},
			expansion: "`id` > ?",
			params:    []interface{}{1000},
		},
		{
			name:      "=",
			left:      []string{"customer_id", "id"},
			operator:  "=",
			right:     []interface{}{1000, 2000},
			expansion: "`customer_id` = ? and `id` = ?",
			params:    []interface{}{1000, 2000},
		},
		{
			name:      ">=",
			left:      []string{"customer_id", "id"},
			operator:  ">=",
			right:     []interface{}{1000, 2000},
			expansion: "`customer_id` > ? or (`customer_id` = ? and `id` >= ?)",
			params:    []interface{}{1000, 1000, 2000},
		},
		{
			name:      "<",
			left:      []string{"customer_id", "id"},
			operator:  "<",
			right:     []interface{}{1000, 2000},
			expansion: "`customer_id` < ? or (`customer_id` = ? and `id` < ?)",
			params:    []interface{}{1000, 1000, 2000},
		},
		{
			name:      "<=",
			left:      []string{"customer_id", "id"},
			operator:  "<=",
			right:     []interface{}{1000, 2000},
			expansion: "`customer_id` < ? or (`customer_id` = ? and `id` <= ?)",
			params:    []interface{}{1000, 1000, 2000},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			expansion, params := expandRowConstructorComparison(test.left, test.operator, test.right)
			assert.Equal(t, test.expansion, expansion)
			assert.Equal(t, test.params, params)
		})
	}
}
