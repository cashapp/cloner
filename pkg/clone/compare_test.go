package clone

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenericCompare(t *testing.T) {
	assert.Equal(t, genericCompare("a", "b"), -1)
	assert.Equal(t, genericCompare("a", "a"), 0)
	assert.Equal(t, genericCompare("b", "a"), 1)

	assert.Equal(t, genericCompare("a", []byte("b")), -1)
	assert.Equal(t, genericCompare("a", []byte("a")), 0)
	assert.Equal(t, genericCompare("b", []byte("a")), 1)

	assert.Equal(t, genericCompare(1001, 1000), 1)
	assert.Equal(t, genericCompare(1000, 1000), 0)
	assert.Equal(t, genericCompare(1000, 1001), -1)

	// A few weird corner cases comparing uintNN and intNN
	assert.Equal(t, genericCompare(math.MaxUint32-1, uint64(math.MaxUint64)), -1)

	assert.Equal(t, genericCompare(math.MinInt, uint64(math.MaxUint32)), -1)
	assert.Equal(t, genericCompare(uint64(math.MaxUint32), math.MinInt), 1)

	assert.Equal(t, genericCompare(uint64(math.MaxUint64), int64(math.MaxInt64)), 1)

	assert.Equal(t, genericCompare(math.MinInt, math.MinInt), 0)

	assert.Panics(t, func() {
		genericCompare(math.MinInt, uint64(math.MaxUint64))
	})
	assert.Panics(t, func() {
		genericCompare(uint64(math.MaxUint64), math.MinInt)
	})
}
