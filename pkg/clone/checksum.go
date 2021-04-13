package clone

import (
	"context"
	log "github.com/sirupsen/logrus"
	_ "net/http/pprof"
	"time"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/sync/errgroup"
)

type Checksum struct {
	ReaderConfig
}

// Run applies the necessary changes to target to make it look like source
func (cmd *Checksum) Run() error {
	var err error

	err = cmd.ReaderConfig.LoadConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	log.WithField("config", cmd).Infof("using config")

	diffs, err := cmd.run()
	if err != nil {
		return errors.WithStack(err)
	}
	if len(diffs) > 0 {
		return errors.Errorf("Found diffs")
	}
	return nil
}

func (cmd *Checksum) run() ([]Diff, error) {
	var err error

	// TODO timeout?
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceReader, err := cmd.Source.ReaderDB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Refresh connections regularly so they don't go stale
	sourceReader.SetConnMaxLifetime(time.Minute)
	sourceReaderCollector := sqlstats.NewStatsCollector("source_reader", sourceReader)
	prometheus.MustRegister(sourceReaderCollector)
	defer prometheus.Unregister(sourceReaderCollector)

	// Target reader
	// We can use a connection pool of unsynced connections for the target because the assumption is there are no
	// other writers to the target during the clone
	targetReader, err := cmd.Target.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Refresh connections regularly so they don't go stale
	targetReader.SetConnMaxLifetime(time.Minute)
	targetReaderCollector := sqlstats.NewStatsCollector("target_reader", targetReader)
	prometheus.MustRegister(targetReaderCollector)
	defer prometheus.Unregister(targetReaderCollector)

	// Load tables
	tables, err := LoadTables(ctx, cmd.ReaderConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Parse the keyrange on the source so that we can filter the target
	shardingSpec, err := cmd.Source.ShardingKeyrange()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	chunks := make(chan Chunk, cmd.ChunkSize)
	diffs := make(chan Diff, cmd.ChunkSize)
	g, ctx := errgroup.WithContext(ctx)

	// TODO the parallelism here could be refactored, we should do like we do in processTable, one table at the time

	// Generate chunks of all source tables
	g.Go(func() error {
		g, ctx := errgroup.WithContext(ctx)
		for _, t := range tables {
			table := t
			g.Go(func() error {
				return GenerateTableChunks(ctx, cmd.ReaderConfig, sourceReader, table, chunks)
			})
		}
		err := g.Wait()
		if err != nil {
			return errors.WithStack(err)
		}
		close(chunks)
		return nil
	})

	readerLimiter := makeLimiter("read_limiter")

	// Forward chunks to differs
	g.Go(func() error {
		g, ctx := errgroup.WithContext(ctx)
		for c := range chunks {
			chunk := c
			acquireTimer := prometheus.NewTimer(readLimiterDelay)
			token, ok := readerLimiter.Acquire(ctx)
			if !ok {
				if token != nil {
					token.OnDropped()
				}
				return errors.Errorf("reader limiter short circuited")
			}
			acquireTimer.ObserveDuration()
			g.Go(func() (err error) {
				defer func() {
					if err == nil {
						token.OnSuccess()
					} else {
						token.OnDropped()
					}
				}()
				err = diffChunk(ctx, cmd.ReaderConfig, sourceReader, targetReader, shardingSpec, chunk, diffs)
				return errors.WithStack(err)
			})
		}
		err := g.Wait()
		if err != nil {
			return errors.WithStack(err)
		}

		// All diffing done, close the diffs channel
		close(diffs)
		return nil
	})

	// Reporter
	var foundDiffs []Diff
	g.Go(func() error {
		for diff := range diffs {
			foundDiffs = append(foundDiffs, diff)
		}
		return nil
	})

	return foundDiffs, g.Wait()
}
