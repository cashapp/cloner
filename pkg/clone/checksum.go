package clone

import (
	"context"
	"database/sql"
	"net/http"
	_ "net/http/pprof"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/topodata"
)

type Checksum struct {
	HighFidelity bool `help:"Clone at a specific GTID using consistent snapshot" default:"false"`

	QueueSize      int `help:"Queue size of the chunk queue" default:"1000"`
	ChunkSize      int `help:"Size of the chunks to diff" default:"1000"`
	WriteBatchSize int `help:"Size of the write batches" default:"100"`
	ChunkerCount   int `help:"Number of readers for chunks" default:"10"`
	ReaderCount    int `help:"Number of readers for diffing" default:"10"`
	WriterCount    int `help:"Number of writers" default:"10"`
}

// Run applies the necessary changes to target to make it look like source
func (cmd *Checksum) Run(globals Globals) error {
	go func() {
		log.Infof("Serving diagnostics on http://localhost:6060")
		err := http.ListenAndServe("localhost:6060", nil)
		log.Fatalf("%v", err)
	}()

	diffs, err := cmd.run(globals)
	if err != nil {
		return errors.WithStack(err)
	}
	if len(diffs) > 0 {
		return errors.Errorf("Found diffs")
	}
	return nil
}

func (cmd *Checksum) run(globals Globals) ([]Diff, error) {
	var err error

	// TODO timeout?
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	source, err := globals.Source.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	target, err := globals.Target.DB()
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Create synced source and chunker conns
	var sourceConns []*sql.Conn
	if cmd.HighFidelity {
		sourceConns, err = OpenSyncedConnections(ctx, source, cmd.ChunkerCount+cmd.ReaderCount)
		// TODO sync up the target connections
		if err != nil {
			return nil, errors.WithStack(err)
		}
	} else {
		sourceConns, err = OpenConnections(ctx, source, cmd.ChunkerCount+cmd.ReaderCount)
		if err != nil {
			return nil, errors.WithStack(err)
		}
	}
	defer CloseConnections(sourceConns)

	chunkerConns := sourceConns[:cmd.ChunkerCount]
	sourceConns = sourceConns[cmd.ChunkerCount:]

	// Create synced target reader conns
	targetConns, err := OpenConnections(ctx, target, cmd.ReaderCount)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	defer CloseConnections(targetConns)

	// Load tables
	sourceVitessTarget, err := parseTarget(globals.Source.Database)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	tables, err := LoadTables(ctx, globals.Source.Type, chunkerConns[0], sourceVitessTarget.Keyspace)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	// Parse the keyrange on the source so that we can filter the target
	shardingSpec, err := key.ParseShardingSpec(sourceVitessTarget.Shard)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	chunks := make(chan Chunk, cmd.QueueSize)
	diffs := make(chan Diff, cmd.QueueSize)
	g, ctx := errgroup.WithContext(ctx)

	// Generate chunks of all source tables
	g.Go(func() error {
		return GenerateChunks(ctx, chunkerConns, tables, cmd.ChunkSize, chunks)
	})

	// Diff chunks
	g.Go(func() error {
		err := differs(ctx, sourceConns, targetConns, shardingSpec, chunks, diffs)
		close(diffs)
		return err
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

// differs runs all the differs in parallel and returns when they are all done
func differs(
	ctx context.Context,
	sourceConns []*sql.Conn,
	targetConns []*sql.Conn,
	shardingSpec []*topodata.KeyRange,
	chunks chan Chunk,
	diffs chan Diff,
) error {
	g, ctx := errgroup.WithContext(ctx)
	for i, _ := range sourceConns {
		sourceConn := sourceConns[i]
		targetConn := targetConns[i]
		g.Go(func() error {
			err := DiffChunks(ctx, sourceConn, targetConn, shardingSpec, chunks, diffs)
			return err
		})
	}
	return g.Wait()
}