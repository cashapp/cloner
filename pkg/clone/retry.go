package clone

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"runtime/debug"
	"time"
)

// Retry retries with back off
func Retry(ctx context.Context, maxRetries uint64, timeout time.Duration, f func(context.Context) error) error {
	start := time.Now()
	retries := 0
	b := backoff.WithContext(backoff.WithMaxRetries(IndefiniteExponentialBackOff(), maxRetries), ctx)
	err := backoff.RetryNotify(func() (err error) {
		debug.SetPanicOnFault(true)
		defer func() {
			if r := recover(); r != nil {
				logrus.WithContext(ctx).Warnf("panic in query, retrying: %v", r)
				err = errors.Errorf("panic in query, retrying: %v", r)
			}
		}()

		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		err = f(ctx)

		return err
	}, b, func(err error, duration time.Duration) {
		retries++
	})

	return errors.Wrapf(err, "failed after %d retries and total duration of %v", retries, time.Since(start))
}

func IndefiniteExponentialBackOff() *backoff.ExponentialBackOff {
	exponentialBackOff := backoff.NewExponentialBackOff()
	exponentialBackOff.MaxInterval = 5 * time.Minute
	exponentialBackOff.MaxElapsedTime = 0
	return exponentialBackOff
}
