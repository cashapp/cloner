package clone

import (
	"context"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
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

	log.Infof("using config: %v", cmd)

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
	g, ctx := errgroup.WithContext(ctx)

	diffs := make(chan Diff)
	// Reporter
	var foundDiffs []Diff
	g.Go(func() error {
		for diff := range diffs {
			foundDiffs = append(foundDiffs, diff)
		}
		return nil
	})

	reader, err := NewReader(cmd.ReaderConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer reader.Close()

	err = reader.Diff(ctx, g, diffs)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return foundDiffs, g.Wait()
}
