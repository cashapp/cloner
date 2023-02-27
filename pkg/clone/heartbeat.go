package clone

import (
	"context"
	"database/sql"
	"fmt"
	_ "net/http/pprof"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/cenkalti/backoff/v4"
	"github.com/mightyguava/autotx"
	"github.com/pkg/errors"
)

// HeartbeatWaitCutOff is the cutoff for when we wait for the heartbeat to arrive to get a more exact lag
const HeartbeatWaitCutOff = time.Minute

// ErrHeartbeatNotFound is the error used when the heartbeat isn't found on the target (replication may have stopped)
var /*const*/ ErrHeartbeatNotFound = errors.New("not found yet, keep retrying")

// Heartbeat receives transactions and requests to snapshot and writes transactions and strongly consistent chunk snapshots
type Heartbeat struct {
	config Replicate
	source *sql.DB
	target *sql.DB

	sourceRetry RetryOptions
	targetRetry RetryOptions
}

func NewHeartbeat(config Replicate) (*Heartbeat, error) {
	var err error
	r := Heartbeat{
		config: config,
		sourceRetry: RetryOptions{
			Limiter:       nil, // will we ever use concurrency limiter again? probably not?
			AcquireMetric: readLimiterDelay.WithLabelValues("source"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
		targetRetry: RetryOptions{
			Limiter:       nil, // will we ever use concurrency limiter again? probably not?
			AcquireMetric: readLimiterDelay.WithLabelValues("target"),
			MaxRetries:    config.ReadRetries,
			Timeout:       config.ReadTimeout,
		},
	}

	source, err := r.config.Source.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.source = source

	target, err := r.config.Target.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	r.target = target

	return &r, nil
}

func (h *Heartbeat) Init(ctx context.Context) error {
	err := h.source.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}
	err = h.target.PingContext(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	if h.config.CreateTables {
		err = h.createTable(ctx)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return nil
}

func (h *Heartbeat) Run(ctx context.Context, b backoff.BackOff) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(h.config.HeartbeatFrequency):
			// Write a heartbeat to the source database
			writeTime := time.Now().UTC()
			count, err := h.write(ctx)
			if err != nil {
				return errors.WithStack(err)
			}

			// Read the current heartbeat time from the source database
			lag, err := h.readLag(ctx)
			if err != nil {
				return errors.WithStack(err)
			}

			if lag < HeartbeatWaitCutOff {
				// If the lag is short we can wait for the heartbeat to arrive to get a more exact number
				err = h.waitForHeartbeat(ctx, count)
				if err != nil {
					if !errors.Is(err, ErrHeartbeatNotFound) {
						return errors.WithStack(err)
					}
				}
				if !errors.Is(err, ErrHeartbeatNotFound) {
					// If we didn't find the heartbeat then replication may have stopped,
					// so we use the difference between source and target heartbeat instead
					lag = time.Now().UTC().Sub(writeTime)
				}
			}

			replicationLag.WithLabelValues(h.config.TaskName).Set(float64(lag / time.Second))
			heartbeatsRead.WithLabelValues(h.config.TaskName).Inc()
			if h.config.LogReplicationLag {
				logrus.
					WithField("task", "heartbeat").
					WithField("replication-lag-seconds", float64(lag/time.Second)).
					Infof("replication lag: %v", lag)
			}

			b.Reset()
		}
	}
}

func (h *Heartbeat) createTable(ctx context.Context) error {
	// TODO retries with backoff?
	timeoutCtx, cancel := context.WithTimeout(ctx, h.config.WriteTimeout)
	defer cancel()
	stmt := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
		  task VARCHAR(255) NOT NULL,
		  time TIMESTAMP NOT NULL,
		  count BIGINT(20) NOT NULL,
		  PRIMARY KEY (task)
		)
		`, "`"+h.config.HeartbeatTable+"`")
	_, err := h.source.ExecContext(timeoutCtx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not create heartbeat table in source database:\n%s", stmt)
	}
	_, err = h.target.ExecContext(timeoutCtx, stmt)
	if err != nil {
		return errors.Wrapf(err, "could not create heartbeat table in target database:\n%s", stmt)
	}
	return nil
}

func (h *Heartbeat) write(ctx context.Context) (int64, error) {
	var count int64
	err := Retry(ctx, h.sourceRetry, func(ctx context.Context) error {
		return autotx.Transact(ctx, h.source, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "SET time_zone = \"+00:00\"")
			if err != nil {
				return errors.WithStack(err)
			}

			row := tx.QueryRowContext(ctx,
				fmt.Sprintf("SELECT count FROM %s WHERE task = ?", h.config.HeartbeatTable), h.config.TaskName)
			err = row.Scan(&count)
			if err != nil {
				if errors.Is(err, sql.ErrNoRows) {
					// We haven't written the first heartbeat yet
					count = 0
				} else {
					return errors.WithStack(err)
				}
			}
			heartbeatTime := time.Now().UTC()
			count += 1
			_, err = tx.ExecContext(ctx,
				fmt.Sprintf("REPLACE INTO %s (task, time, count) VALUES (?, ?, ?)",
					h.config.HeartbeatTable),
				h.config.TaskName, heartbeatTime, count)
			return errors.WithStack(err)
		})
	})

	return count, errors.WithStack(err)
}

func (h *Heartbeat) findHeartbeat(ctx context.Context, count int64) (bool, error) {
	found := false

	err := Retry(ctx, h.targetRetry, func(ctx context.Context) error {
		stmt := fmt.Sprintf("SELECT 1 FROM %s WHERE task = ? AND count >= ?", h.config.HeartbeatTable)
		row := h.target.QueryRowContext(ctx, stmt, h.config.TaskName, count)
		var dummy int
		err := row.Scan(&dummy)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				found = false
				return nil
			}

			if isSchemaError(err) {
				return backoff.Permanent(err)
			}
			return errors.WithStack(err)
		}
		found = true
		return nil
	})

	return found, errors.WithStack(err)
}

func (h *Heartbeat) waitForHeartbeat(ctx context.Context, count int64) error {
	b := backoff.NewExponentialBackOff()
	b.MaxInterval = HeartbeatWaitCutOff / 10
	b.MaxElapsedTime = 2 * HeartbeatWaitCutOff

	err := backoff.Retry(func() error {
		found, err := h.findHeartbeat(ctx, count)
		if err != nil {
			if isSchemaError(err) {
				return backoff.Permanent(err)
			}
			return errors.WithStack(err)
		}
		if !found {
			return ErrHeartbeatNotFound
		}
		// Yay, we found the heartbeat!
		return nil
	}, backoff.WithContext(b, ctx))

	if err != nil {
		var permanent backoff.PermanentError
		if errors.Is(err, &permanent) {
			err = errors.Unwrap(err)
		}
		return errors.Wrapf(err, "failed to read heartbeat")
	}

	// Heartbeat errors are ignored
	return nil
}

func (h *Heartbeat) readLag(ctx context.Context) (time.Duration, error) {
	lag := time.Hour
	err := Retry(ctx, h.targetRetry, func(ctx context.Context) error {
		stmt := fmt.Sprintf("SELECT time FROM %s WHERE task = ?", h.config.HeartbeatTable)
		row := h.target.QueryRowContext(ctx, stmt, h.config.TaskName)
		var lastHeartbeat time.Time
		err := row.Scan(&lastHeartbeat)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				// We haven't received the first heartbeat yet, maybe we're an hour behind, who knows?
				// We're definitely more than 0 ms so let's go with one hour just to pick a number >0
				lag = time.Hour
			} else {
				return errors.WithStack(err)
			}
		} else {
			lag = time.Now().UTC().Sub(lastHeartbeat)
		}
		return nil
	})
	if err != nil {
		return lag, errors.WithStack(err)
	}
	return lag, nil
}
