package clone

import (
	"context"
	log "github.com/sirupsen/logrus"
	_ "net/http/pprof"
	"time"

	"github.com/pkg/errors"
)

type Checksum struct {
	ReaderConfig
}

// Run finds any differences between source and target
func (cmd *Checksum) Run() error {
	var err error

	start := time.Now()

	err = cmd.ReaderConfig.LoadConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	log.WithField("config", cmd).Infof("using config")

	diffs, err := cmd.run()

	elapsed := time.Since(start)
	logger := log.WithField("duration", elapsed)
	if err != nil {
		if stackErr, ok := err.(stackTracer); ok {
			logger = logger.WithField("stacktrace", stackErr.StackTrace())
		}
		logger.WithError(err).Errorf("error: %+v", err)
	} else {
		logger.Infof("full checksum success")
	}

	if len(diffs) > 0 {
		// TODO log a more detailed diff report
		return errors.Errorf("found diffs")
	}
	return errors.WithStack(err)
}

func (cmd *Checksum) run() ([]Diff, error) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	diffs := make(chan Diff)
	// Reporter
	var foundDiffs []Diff
	go func() {
		for diff := range diffs {
			foundDiffs = append(foundDiffs, diff)
		}
	}()

	reader := NewReader(cmd.ReaderConfig)
	err := reader.Diff(ctx, diffs)

	return foundDiffs, err
}
