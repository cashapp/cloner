package clone

import (
	"github.com/platinummonkey/go-concurrency-limits/core"
	"github.com/platinummonkey/go-concurrency-limits/limiter"
	"github.com/platinummonkey/go-concurrency-limits/strategy"
	"github.com/sirupsen/logrus"
)

type limitLogger struct {
}

func (l limitLogger) Debugf(msg string, params ...interface{}) {
	logrus.Debugf(msg, params...)
}

func (l limitLogger) IsDebugEnabled() bool {
	return logrus.IsLevelEnabled(logrus.DebugLevel)
}

func makeLimiter(name string) core.Limiter {
	limitStrategy := strategy.NewSimpleStrategy(10)
	logger := limitLogger{}
	defaultLimiter, err := limiter.NewDefaultLimiterWithDefaults(
		name,
		limitStrategy,
		logger,
		core.EmptyMetricRegistryInstance,
	)
	if err != nil {
		logrus.Panicf("failed to create limiter: %s", err)
	}
	return limiter.NewBlockingLimiter(defaultLimiter, 0, logger)
}
