package clone

import (
	"context"
	"database/sql"
	"fmt"
	_ "net/http/pprof"
	"strings"

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
	WriteRetryCount int      `help:"Number of writers" default:"5"`
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
	tables, err := LoadTables(ctx, globals.Source.Type, chunkerConns[0], sourceVitessTarget.Keyspace, cmd.Tables)
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

	// Start writers
	writeRequests := make(chan Batch, cmd.QueueSize)
	for i, _ := range writerConns {
		conn := writerConns[i]
		g.Go(func() error {
			return Write(ctx, cmd, conn, writeRequests)
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
		err := readers(ctx, chunkerConns, tableCh, cmd, writeRequests, diffRequests)
		// All tables are done, close the channels used to request work
		close(writeRequests)
		close(diffRequests)
		return err
	})

	err = g.Wait()
	log.Infof("done cloning %d tables", len(tables))
	return err
}

// readers runs all the readers in parallel and returns when they are all done
func readers(
	ctx context.Context,
	chunkerConns []*sql.Conn,
	tableCh chan *Table,
	cmd *Clone,
	batches chan Batch,
	diffRequests chan DiffRequest,
) error {
	g, ctx := errgroup.WithContext(ctx)
	for i, _ := range chunkerConns {
		chunkerConn := chunkerConns[i]
		g.Go(func() error {
			err := ReadTables(ctx, chunkerConn, tableCh, cmd, batches, diffRequests)
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
