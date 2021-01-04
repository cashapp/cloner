package clone

import (
	"context"
	_ "net/http/pprof"
	"time"

	"github.com/dlmiddlecote/sqlstats"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
)

type Clone struct {
	ReaderConfig

	Consistent bool `help:"Clone at a specific GTID using consistent snapshot" default:"false"`

	NoDiff bool `help:"Clone without diffing" default:"false"`

	WriteBatchSize  int           `help:"Size of the write batches" default:"100"`
	WriterCount     int           `help:"Number of writer connections" default:"10"`
	WriteRetryCount uint64        `help:"Number of retries" default:"5"`
	WriteTimeout    time.Duration `help:"Timeout for each write" default:"30s"`
}

// Run applies the necessary changes to target to make it look like source
func (cmd *Clone) Run() error {
	start := time.Now()

	var err error

	g, ctx := errgroup.WithContext(context.Background())

	// Create synced reader conns
	if cmd.Consistent {
		// TODO the way to do this is to create synced connections and then implement a pool implementing DBReader
		//      that uses  those connections
		return errors.Errorf("consistent cloning not currently supported")
	}

	sourceReader, err := cmd.Source.ReaderDB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer sourceReader.Close()
	// Refresh connections regularly so they don't go stale
	sourceReader.SetConnMaxLifetime(time.Minute)
	sourceReaderCollector := sqlstats.NewStatsCollector("source_reader", sourceReader)
	prometheus.MustRegister(sourceReaderCollector)
	defer prometheus.Unregister(sourceReaderCollector)

	writer, err := cmd.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer writer.Close()
	// Refresh connections regularly so they don't go stale
	writer.SetConnMaxLifetime(time.Minute)
	writerCollector := sqlstats.NewStatsCollector("target_writer", writer)
	prometheus.MustRegister(writerCollector)
	defer prometheus.Unregister(writerCollector)

	// Target reader
	// We can use a connection pool of unsynced connections for the target because the assumption is there are no
	// other writers to the target during the clone
	targetReader, err := cmd.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer targetReader.Close()
	// Refresh connections regularly so they don't go stale
	targetReader.SetConnMaxLifetime(time.Minute)
	targetReaderCollector := sqlstats.NewStatsCollector("target_reader", targetReader)
	prometheus.MustRegister(targetReaderCollector)
	defer prometheus.Unregister(targetReaderCollector)

	// Load tables
	// TODO in consistent clone we should diff the schema of the source with the target,
	//      for now we just use the target schema
	tables, err := LoadTables(ctx, cmd.ReaderConfig)
	if err != nil {
		return errors.WithStack(err)
	}

	logger := log.WithField("tables", len(tables))

	shardingSpec, err := cmd.Source.ShardingKeyrange()
	if err != nil {
		return errors.WithStack(err)
	}

	// Queue up tables to read
	tableCh := make(chan *Table, len(tables))
	for _, table := range tables {
		tableCh <- table
	}
	close(tableCh)

	writerLimiter := makeLimiter("write_limiter")
	readerLimiter := makeLimiter("read_limiter")

	if cmd.TableParallelism == 0 {
		return errors.Errorf("need more parallelism")
	}

	logger.Infof("starting clone %s -> %s", cmd.Source.String(), cmd.Target.String())

	// Chunk, diff table and generate batches to write
	tableLimiter := semaphore.NewWeighted(int64(cmd.TableParallelism))
	for _, t := range tables {
		table := t
		g.Go(func() error {
			err := tableLimiter.Acquire(ctx, 1)
			if err != nil {
				return errors.WithStack(err)
			}
			defer tableLimiter.Release(1)
			err = processTable(ctx, sourceReader, targetReader, table, cmd, writer, writerLimiter, readerLimiter, shardingSpec)
			return errors.WithStack(err)
		})
	}

	err = g.Wait()

	elapsed := time.Since(start)
	logger = log.WithField("duration", elapsed).WithField("tables", len(tables))
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

type stackTracer interface {
	StackTrace() errors.StackTrace
}
