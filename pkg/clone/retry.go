package clone

import (
	"context"
	"runtime/debug"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
)

type RetryOptions struct {
	Limiter       core.Limiter
	AcquireMetric prometheus.Observer
	MaxRetries    uint64
	Timeout       time.Duration
}

// Retry retries with back off
func Retry(ctx context.Context, options RetryOptions, f func(context.Context) error) error {
	start := time.Now()
	retries := 0
	b := backoff.WithContext(backoff.WithMaxRetries(IndefiniteExponentialBackOff(), options.MaxRetries), ctx)
	operation := func() (err error) {
		debug.SetPanicOnFault(true)
		defer func() {
			if r := recover(); r != nil {
				logrus.WithField("stack", string(debug.Stack())).
					WithField("panic", r).
					WithContext(ctx).
					Warnf("panic in query, retrying: %v", r)
				err = errors.Errorf("panic in query, retrying: %v", r)
			}
		}()

		if options.Limiter != nil {
			acquireTimer := prometheus.NewTimer(options.AcquireMetric)
			token, ok := options.Limiter.Acquire(ctx)
			if !ok {
				if token != nil {
					token.OnDropped()
				}
				if ctx.Err() != nil {
					return errors.Wrap(ctx.Err(), "context deadline exceeded")
				} else {
					return errors.New("context deadline exceeded")
				}
			}
			acquireTimer.ObserveDuration()

			defer func() {
				if err == nil {
					token.OnSuccess()
				} else {
					token.OnDropped()
				}
			}()
		}

		ctx, cancel := context.WithTimeout(ctx, options.Timeout)
		defer cancel()

		err = f(ctx)

		if isSchemaError(err) {
			logrus.Infof("schema error, returning without retrying")
			return backoff.Permanent(err)
		}

		return err
	}

	err := backoff.RetryNotify(operation, b, func(err error, duration time.Duration) {
		logrus.Warnf("failed operation due to %v, will retry after %s", err, duration)
		retries++
	})

	return errors.Wrapf(err, "failed after %d retries and total duration of %v", retries, time.Since(start))
}

func IndefiniteExponentialBackOff() *backoff.ExponentialBackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.MaxInterval = 1 * time.Minute
	exponentialBackOff.MaxElapsedTime = 0 // never stops
	return exponentialBackOff
}
