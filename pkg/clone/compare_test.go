package clone

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGenericCompare(t *testing.T) {
	assert.Equal(t, genericCompareOrPanic("a", "b"), -1)
	assert.Equal(t, genericCompareOrPanic("a", "a"), 0)
	assert.Equal(t, genericCompareOrPanic("b", "a"), 1)

	assert.Equal(t, genericCompareOrPanic("a", []byte("b")), -1)
	assert.Equal(t, genericCompareOrPanic("a", []byte("a")), 0)
	assert.Equal(t, genericCompareOrPanic("b", []byte("a")), 1)

	assert.Equal(t, genericCompareOrPanic(1001, 1000), 1)
	assert.Equal(t, genericCompareOrPanic(1000, 1000), 0)
	assert.Equal(t, genericCompareOrPanic(1000, 1001), -1)

	// A few weird corner cases comparing uintNN and intNN
	assert.Equal(t, genericCompareOrPanic(math.MaxUint32-1, uint64(math.MaxUint64)), -1)

	assert.Equal(t, genericCompareOrPanic(math.MinInt, uint64(math.MaxUint32)), -1)
	assert.Equal(t, genericCompareOrPanic(uint64(math.MaxUint32), math.MinInt), 1)

	assert.Equal(t, genericCompareOrPanic(uint64(math.MaxUint64), int64(math.MaxInt64)), 1)

	assert.Equal(t, genericCompareOrPanic(math.MinInt, math.MinInt), 0)

	_, err := genericCompare(math.MinInt, uint64(math.MaxUint64))
	assert.Errorf(t, err, "")
	_, err = genericCompare(uint64(math.MaxUint64), math.MinInt)
	assert.Errorf(t, err, "")
}
