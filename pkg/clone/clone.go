package clone

import (
	"context"
	_ "net/http/pprof"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

type Clone struct {
	WriterConfig
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// Run finds any differences between source and target
func (cmd *Clone) Run() error {
	var err error

	start := time.Now()

	err = cmd.ReaderConfig.LoadConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	log.Infof("using config: %v", cmd)

	err = cmd.run()

	elapsed := time.Since(start)
	logger := log.WithField("duration", elapsed)
	if err != nil {
		if stackErr, ok := err.(stackTracer); ok {
			logger = logger.WithField("stacktrace", stackErr.StackTrace())
		}
		logger.WithError(err).Errorf("error: %+v", err)
	} else {
		logger.Infof("full clone success")
	}

	return errors.WithStack(err)
}

func (cmd *Clone) run() error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g, ctx := errgroup.WithContext(ctx)

	diffs := make(chan Diff)

	writer := NewWriter(cmd.WriterConfig)
	defer writer.Close()
	err := writer.Write(ctx, g, diffs)
	if err != nil {
		return errors.WithStack(err)
	}

	reader := NewReader(cmd.ReaderConfig)
	defer reader.Close()
	if cmd.NoDiff {
		err = reader.Read(ctx, g, diffs)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		err = reader.Diff(ctx, g, diffs)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	return g.Wait()
}
