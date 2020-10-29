package clone

import (
	"context"
	"database/sql"
	_ "net/http/pprof"
	"sync"
	"time"

	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
	"vitess.io/vitess/go/vt/key"
)

type Checksum struct {
	QueueSize    int           `help:"Queue size of the chunk queue" default:"1000"`
	ChunkSize    int           `help:"Size of the chunks to diff" default:"1000"`
	ChunkerCount int           `help:"Number of readers for chunks" default:"10"`
	ReaderCount  int           `help:"Number of readers for diffing" default:"10"`
	ReadTimeout  time.Duration `help:"Timeout for each read" default:"5m"`
	Tables       []string      `help:"Tables to checksum (if unset will clone all of them)" optional:""`
}

// Run applies the necessary changes to target to make it look like source
func (cmd *Checksum) Run(globals Globals) error {
	globals.startMetricsServer()

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
	sourceConns, err = OpenConnections(ctx, source, cmd.ChunkerCount+cmd.ReaderCount)
	if err != nil {
		return nil, errors.WithStack(err)
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
	tables, err := LoadTables(ctx, globals.Source.Type, chunkerConns[0], sourceVitessTarget.Keyspace, isSharded(sourceVitessTarget), cmd.Tables)
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

	// Start differs
	diffRequests := make(chan DiffRequest, cmd.QueueSize)
	for i, _ := range targetConns {
		source := sourceConns[i]
		target := targetConns[i]
		g.Go(func() error {
			return DiffChunks(ctx, source, target, shardingSpec, cmd.ReadTimeout, diffRequests)
		})
	}

	// Generate chunks of all source tables
	g.Go(func() error {
		err := GenerateChunks(ctx, chunkerConns, tables, cmd.ChunkSize, chunks)
		close(chunks)
		return errors.WithStack(err)
	})

	// Forward chunks to differs
	g.Go(func() error {
		done := &sync.WaitGroup{}
		for chunk := range chunks {
			done.Add(1)
			diffRequests <- DiffRequest{chunk, diffs, done}
		}
		WaitGroupWait(ctx, done)
		close(diffRequests)
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
