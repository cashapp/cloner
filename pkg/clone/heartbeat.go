package clone

import (
	"context"
	"database/sql"
	"fmt"
	_ "net/http/pprof"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/mightyguava/autotx"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

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
			err := h.write(ctx)
			if err != nil {
				return errors.WithStack(err)
			}
			err = h.read(ctx)
			if err != nil {
				return errors.WithStack(err)
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

func (h *Heartbeat) write(ctx context.Context) error {
	err := Retry(ctx, h.sourceRetry, func(ctx context.Context) error {
		return autotx.Transact(ctx, h.source, func(tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, "SET time_zone = \"+00:00\"")
			if err != nil {
				return errors.WithStack(err)
			}

			row := tx.QueryRowContext(ctx,
				fmt.Sprintf("SELECT count FROM %s WHERE task = ?", h.config.HeartbeatTable), h.config.TaskName)
			var count int64
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
			_, err = tx.ExecContext(ctx,
				fmt.Sprintf("REPLACE INTO %s (task, time, count) VALUES (?, ?, ?)",
					h.config.HeartbeatTable),
				h.config.TaskName, heartbeatTime, count+1)
			return errors.WithStack(err)
		})
	})

	return errors.WithStack(err)
}

func (h *Heartbeat) read(ctx context.Context) error {
	logger := logrus.WithContext(ctx).WithField("task", "heartbeat")

	err := Retry(ctx, h.targetRetry, func(ctx context.Context) error {
		stmt := fmt.Sprintf("SELECT time FROM %s WHERE task = ?", h.config.HeartbeatTable)
		row := h.target.QueryRowContext(ctx, stmt, h.config.TaskName)
		var lastHeartbeat time.Time
		var lag time.Duration
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
			heartbeatsRead.WithLabelValues(h.config.TaskName).Inc()
			lag = time.Now().UTC().Sub(lastHeartbeat)
		}
		replicationLag.WithLabelValues(h.config.TaskName).Set(float64(lag / time.Second))
		return nil
	})
	if err != nil {
		logger.WithError(err).Errorf("failed to read heartbeat: %v", err)
	}
	// Heartbeat errors are ignored
	return nil
}
