package clone

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

type Clone struct {
	HighFidelity bool `help:"Clone at a specific GTID using consistent snapshot" default:"false"`

	QueueSize      int  `help:"Queue size of the chunk queue" default:"1000"`
	ChunkSize      int  `help:"Size of the chunks to diff" default:"1000"`
	WriteBatchSize int  `help:"Size of the write batches" default:"100"`
	ChunkerCount   int  `help:"Number of readers for chunks" default:"10"`
	ReaderCount    int  `help:"Number of readers for diffing" default:"10"`
	WriterCount    int  `help:"Number of writers" default:"10"`
	CopySchema     bool `help:"Copy schema" default:"false"`
}

// Run applies the necessary changes to target to make it look like source
func (cmd *Clone) Run(globals Globals) error {
	go func() {
		log.Infof("Serving diagnostics on http://localhost:6060/metrics and http://localhost:6060/debug/pprof")
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe("localhost:6060", nil)
		log.Fatalf("%v", err)
	}()

	var err error

	// TODO timeout?
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sourceReader, err := globals.Source.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	target, err := globals.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}

	// Create synced source and chunker conns
	var sourceConns []*sql.Conn
	if cmd.HighFidelity {
		sourceConns, err = OpenSyncedConnections(ctx, sourceReader, cmd.ChunkerCount+cmd.ReaderCount)
		if err != nil {
			return err
		}
	} else {
		sourceConns, err = OpenConnections(ctx, sourceReader, cmd.ChunkerCount+cmd.ReaderCount)
		if err != nil {
			return err
		}
	}
	defer CloseConnections(sourceConns)

	chunkerConns := sourceConns[:cmd.ChunkerCount]
	sourceConns = sourceConns[cmd.ChunkerCount:]

	// Create synced target reader conns
	targetConns, err := OpenConnections(ctx, target, cmd.ReaderCount)
	if err != nil {
		return err
	}
	defer CloseConnections(targetConns)

	// Create writer connections
	writerConns, err := OpenConnections(ctx, target, cmd.WriterCount)
	if err != nil {
		return err
	}
	defer CloseConnections(writerConns)

	// Load tables
	sourceVitessTarget, err := parseTarget(globals.Source.Database)
	if err != nil {
		return errors.WithStack(err)
	}
	tables, err := LoadTables(ctx, globals.Source.Type, chunkerConns[0], sourceVitessTarget.Keyspace)
	if err != nil {
		return errors.WithStack(err)
	}

	// Copy schema
	if cmd.CopySchema {
		err := CopySchema(ctx, tables, chunkerConns[0], writerConns[0])
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Parse the keyrange on the source so that we can filter the target
	shardingSpec, err := key.ParseShardingSpec(sourceVitessTarget.Shard)
	if err != nil {
		return errors.WithStack(err)
	}

	chunks := make(chan Chunk, cmd.QueueSize)
	diffs := make(chan Diff, cmd.QueueSize)
	batches := make(chan Batch, cmd.QueueSize)
	errCh := make(chan error)
	wg := &sync.WaitGroup{}
	waitCh := make(chan struct{})

	// Periodically dump metrics
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := PeriodicallyDumpMetrics(ctx)
		if err != nil {
			errCh <- err
		}
	}()

	// Generate chunks of all source tables
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := GenerateChunks(ctx, chunkerConns, tables, cmd.ChunkSize, chunks)
		if err != nil {
			log.WithField("task", "chunker").WithError(err).Errorf("")
			errCh <- err
		}
	}()

	// Batch writes
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := BatchWrites(ctx, cmd.WriteBatchSize, diffs, batches)
		if err != nil {
			log.WithField("task", "batcher").WithError(err).Errorf("")
			errCh <- err
		}
	}()

	// Write batches
	for i, _ := range writerConns {
		conn := writerConns[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := Write(ctx, conn, batches)
			if err != nil {
				log.WithField("task", "writer").WithError(err).Errorf("")
				errCh <- err
			}
		}()
	}

	// Diff chunks
	for i, _ := range sourceConns {
		sourceConn := sourceConns[i]
		targetConn := targetConns[i]
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := DiffChunks(ctx, sourceConn, targetConn, shardingSpec, chunks, diffs)
			if err != nil {
				log.WithField("task", "differ").WithError(err).Errorf("")
				errCh <- err
			}
		}()
	}

	// Wrap the wait group in a channel so that we can select on it below
	go func() {
		wg.Wait()
		close(waitCh)
	}()

	// Wait for everything to complete or we get an error
	select {
	case <-waitCh:
		log.Debugf("Done")
		return nil
	case err := <-errCh:
		cancel() // cancel before we close the channel so we don't panic by writing on a closed channel
		close(errCh)
		// Return the first error only, ignore the rest (they should have logged)
		return err
	case <-ctx.Done():
		return errors.Errorf("Timed out")
	}
}

func PeriodicallyDumpMetrics(ctx context.Context) error {
	ticker := time.NewTicker(60 * time.Second)
	for {
		describe := make(chan *prometheus.Desc)
		writeCounter.Describe(describe)
		description := <-describe
		log.Infof("writes = %v", description)
		select {
		case <-ctx.Done():
			ticker.Stop()
			return nil
		case <-ticker.C:
		}
	}
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
