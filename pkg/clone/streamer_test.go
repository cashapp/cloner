package clone

import (
	"testing"

	"github.com/stretchr/testify/assert"
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
			name:      "> with 1 param",
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
		{
			// For >3 parameters we don't expand
			name:      "<= with 3 params",
			left:      []string{"bank_id", "customer_id", "id"},
			operator:  "<=",
			right:     []interface{}{1000, 2000, 3000},
			expansion: "(`bank_id`,`customer_id`,`id`) <= (?,?,?)",
			params:    []interface{}{1000, 2000, 3000},
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
