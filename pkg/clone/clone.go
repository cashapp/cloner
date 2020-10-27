package clone

import (
	"context"
	"database/sql"
	"fmt"
	_ "net/http/pprof"
	"strings"
	"sync"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

type Clone struct {
	HighFidelity bool `help:"Clone at a specific GTID using consistent snapshot" default:"false"`

	QueueSize       int      `help:"Queue size of the chunk queue" default:"10000"`
	ChunkSize       int      `help:"Size of the chunks to diff" default:"1000"`
	WriteBatchSize  int      `help:"Size of the write batches" default:"100"`
	ChunkerCount    int      `help:"Number of readers for chunks" default:"10"`
	ReaderCount     int      `help:"Number of readers for diffing" default:"10"`
	WriterCount     int      `help:"Number of writers" default:"10"`
	WriteRetryCount int      `help:"Number of retries" default:"5"`
	CopySchema      bool     `help:"Copy schema" default:"false"`
	Tables          []string `help:"Tables to clone (if unset will clone all of them)" optionals:""`
}

// Run applies the necessary changes to target to make it look like source
func (cmd *Clone) Run(globals Globals) error {
	globals.startMetricsServer()

	var err error

	// TODO timeout?
	g, ctx := errgroup.WithContext(context.Background())

	sourceReader, err := globals.Source.ReaderDB()
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

	// Create target reader conns
	// TODO how do we synchronize these, do we have to?
	targetConns, err := OpenConnections(ctx, target, cmd.ReaderCount)
	if err != nil {
		return err
	}
	defer CloseConnections(targetConns)

	// Load tables
	sourceVitessTarget, err := parseTarget(globals.Source.Database)
	if err != nil {
		return errors.WithStack(err)
	}
	tables, err := LoadTables(ctx, globals.Source.Type, chunkerConns[0], sourceVitessTarget.Keyspace, cmd.Tables)
	if err != nil {
		return errors.WithStack(err)
	}

	// Copy schema
	if cmd.CopySchema {
		err := CopySchema(ctx, tables, chunkerConns[0], target)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Parse the keyrange on the source so that we can filter the target
	shardingSpec, err := key.ParseShardingSpec(sourceVitessTarget.Shard)
	if err != nil {
		return errors.WithStack(err)
	}

	// Start writers
	// we might want to wrap up this channel and the WaitGroup in a single object
	writesInFlight := &sync.WaitGroup{}
	writeRequests := make(chan Batch, cmd.QueueSize)
	for i := 0; i < cmd.WriterCount; i++ {
		g.Go(func() error {
			return Write(ctx, cmd, target, writeRequests, writesInFlight)
		})
	}

	// Start differs
	diffRequests := make(chan DiffRequest, cmd.QueueSize)
	for i, _ := range targetConns {
		source := sourceConns[i]
		target := targetConns[i]
		g.Go(func() error {
			return DiffChunks(ctx, source, target, shardingSpec, diffRequests)
		})
	}

	// Queue up tables to read
	tableCh := make(chan *Table, len(tables))
	for _, table := range tables {
		tableCh <- table
	}
	close(tableCh)

	// Chunk, diff table and generate batches to write
	g.Go(func() error {
		err := readers(ctx, chunkerConns, tableCh, cmd, writeRequests, writesInFlight, diffRequests)
		// All tables are done, close the channels used to request work
		close(diffRequests)

		// closing this channel here means we can't use this channel for retries...
		// instead we need to wait for the channel to become empty before we close it?
		WaitGroupWait(ctx, writesInFlight)
		close(writeRequests)

		return err
	})

	err = g.Wait()
	if err != nil {
		log.WithError(err).Errorf("failed cloning %d tables", len(tables))
	} else {
		log.Infof("done cloning %d tables", len(tables))
	}
	return err
}

// readers runs all the readers in parallel and returns when they are all done
func readers(ctx context.Context, chunkerConns []*sql.Conn, tableCh chan *Table, cmd *Clone, writeRequests chan Batch, writesInFlight *sync.WaitGroup, diffRequests chan DiffRequest) error {
	g, ctx := errgroup.WithContext(ctx)
	for i, _ := range chunkerConns {
		chunkerConn := chunkerConns[i]
		g.Go(func() error {
			err := ReadTables(ctx, chunkerConn, tableCh, cmd, writeRequests, writesInFlight, diffRequests)
			return err
		})
	}
	return g.Wait()
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
