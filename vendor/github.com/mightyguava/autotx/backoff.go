package autotx

import (
	"math"
	"time"
)

const (
	// DefaultInitialBackoff configures the initial backoff interval.
	DefaultInitialBackOff = 10 * time.Millisecond
	// DefaultMaxBackoff configures the maximum backoff interval.
	DefaultMaxBackOff = 1 * time.Second
	// DefaultBackOffFactor configues the factor the previous backoff interval will be multiplied by
	// to get the next backoff.
	DefaultBackOffFactor = 2
)

// BackOffFunc is a function called on each retry attempt. It should return a time.Duration to wait
// before making the next attempt. If a negative time.Duration is returned, retries will be immediately
// aborted.
type BackOffFunc func() time.Duration

type simpleBackOff struct {
	attempt  int
	factor   int
	min, max time.Duration
}

func newSimpleExponentialBackOff() *simpleBackOff {
	return &simpleBackOff{
		factor: DefaultBackOffFactor,
		min:    DefaultInitialBackOff,
		max:    DefaultMaxBackOff,
	}
}

func (b *simpleBackOff) NextBackOff() time.Duration {
	next := b.min * time.Duration(math.Pow(float64(b.factor), float64(b.attempt)))
	b.attempt++
	if next > b.max {
		return b.max
	}
	return next
}
