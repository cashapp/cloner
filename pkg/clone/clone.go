package clone

import (
	"context"
	"fmt"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

type Clone struct {
	Consistent bool `help:"Clone at a specific GTID using consistent snapshot" default:"false"`

	QueueSize int `help:"Size of internal queues, increase this number might increase throughput but does increases memory footprint" default:"10000"`

	ChunkSize        int           `help:"Size of the chunks to diff" default:"1000"`
	TableParallelism int           `help:"Number of tables to process concurrently" default:"10"`
	ReaderCount      int           `help:"Number of reader connections" default:"20"`
	ReadTimeout      time.Duration `help:"Timeout for faster reads like diffing a single chunk" default:"30s"`
	ChunkingTimeout  time.Duration `help:"Timeout for the chunking (which can take a really long time)" default:"1h"`

	WriteBatchSize  int           `help:"Size of the write batches" default:"100"`
	WriterCount     int           `help:"Number of writer connections" default:"10"`
	WriteRetryCount int           `help:"Number of retries" default:"5"`
	WriteTimeout    time.Duration `help:"Timeout for each write" default:"30s"`

	Tables []string `help:"Tables to clone (if unset will clone all of them)" optional:""`
}

// Run applies the necessary changes to target to make it look like source
func (cmd *Clone) Run(globals Globals) error {
	globals.startMetricsServer()

	start := time.Now()

	var err error

	// TODO timeout?
	g, ctx := errgroup.WithContext(context.Background())

	// Create synced reader conns
	if cmd.Consistent {
		// TODO the way to do this is to create synced connections and then implement a pool implementing DBReader
		//      that uses  those connections
		return errors.Errorf("consistent cloning not currently supported")
	}

	sourceReader, err := globals.Source.ReaderDB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer sourceReader.Close()
	// Refresh connections regularly so they don't go stale
	sourceReader.SetConnMaxLifetime(time.Minute)

	writer, err := globals.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer writer.Close()
	// Refresh connections regularly so they don't go stale
	writer.SetConnMaxLifetime(time.Minute)

	// Target reader
	// We can use a connection pool of unsynced connections for the target because the assumption is there are no
	// other writers to the target during the clone
	targetReader, err := globals.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer targetReader.Close()
	// Refresh connections regularly so they don't go stale
	targetReader.SetConnMaxLifetime(time.Minute)

	// Load tables
	sourceVitessTarget, err := parseTarget(globals.Source.Database)
	if err != nil {
		return errors.WithStack(err)
	}
	tables, err := LoadTables(ctx, globals.Source.Type, sourceReader, sourceVitessTarget.Keyspace, isSharded(sourceVitessTarget), cmd.Tables, cmd.ReadTimeout)
	if err != nil {
		return errors.WithStack(err)
	}

	logger := log.WithField("tables", len(tables))

	// Parse the keyrange on the source so that we can filter the target
	var shardingSpec []*topodata.KeyRange
	if isSharded(sourceVitessTarget) {
		shardingSpec, err = key.ParseShardingSpec(sourceVitessTarget.Shard)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Queue up tables to read
	tableCh := make(chan *Table, len(tables))
	for _, table := range tables {
		tableCh <- table
	}
	close(tableCh)

	writerLimiter := semaphore.NewWeighted(int64(cmd.WriterCount))
	readerLimiter := semaphore.NewWeighted(int64(cmd.ReaderCount))

	if cmd.TableParallelism == 0 {
		return errors.Errorf("need more parallelism")
	}

	logger.Infof("starting clone %s -> %s", globals.Source.String(), globals.Target.String())

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

func parseTarget(targetString string) (*query.Target, error) {
	// Default tablet type is master.
	target := &query.Target{
		TabletType: topodata.TabletType_MASTER,
	}
	last := strings.LastIndexAny(targetString, "@")
	if last != -1 {
		// No need to check the error. UNKNOWN will be returned on
		// error and it will fail downstream.
		tabletType, err := topoproto.ParseTabletType(targetString[last+1:])
		if err != nil {
			return target, err
		}
		target.TabletType = tabletType
		targetString = targetString[:last]
	}
	last = strings.LastIndexAny(targetString, "/:")
	if last != -1 {
		target.Shard = targetString[last+1:]
		targetString = targetString[:last]
	}
	target.Keyspace = targetString
	if target.Keyspace == "" {
		return target, fmt.Errorf("no keyspace in: %v", targetString)
	}
	if target.Shard == "" {
		return target, fmt.Errorf("no shard in: %v", targetString)
	}
	return target, nil
}
