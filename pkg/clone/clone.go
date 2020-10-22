package clone

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"strings"

	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

type Clone struct {
	HighFidelity bool `help:"Clone at a specific GTID using consistent snapshot" default:"false"`

	QueueSize      int      `help:"Queue size of the chunk queue" default:"1000"`
	ChunkSize      int      `help:"Size of the chunks to diff" default:"1000"`
	WriteBatchSize int      `help:"Size of the write batches" default:"100"`
	ChunkerCount   int      `help:"Number of readers for chunks" default:"10"`
	ReaderCount    int      `help:"Number of readers for diffing" default:"10"`
	WriterCount    int      `help:"Number of writers" default:"10"`
	CopySchema     bool     `help:"Copy schema" default:"false"`
	Tables         []string `help:"Tables to clone (if unset will clone all of them)" optionals:""`
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

	batches := make(chan Batch, cmd.QueueSize)

	// Write batches
	for i, _ := range writerConns {
		conn := writerConns[i]
		g.Go(func() error {
			return Write(ctx, conn, batches)
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
		err := readers(
			ctx,
			chunkerConns,
			sourceConns,
			targetConns,
			shardingSpec,
			tableCh,
			cmd,
			batches,
		)
		// All the readers are done, close the write batches channel
		close(batches)
		return err
	})

	log.Infof("done cloning %d tables", len(tables))

	// Wait for everything to complete or we get an error
	return g.Wait()
}

// readers runs all the readers in parallel and returns when they are all done
func readers(
	ctx context.Context,
	chunkerConns []*sql.Conn,
	sourceConns []*sql.Conn,
	targetConns []*sql.Conn,
	shardingSpec []*topodata.KeyRange,
	tableCh chan *Table,
	cmd *Clone,
	batches chan Batch,
) error {
	g, ctx := errgroup.WithContext(ctx)
	for i, _ := range sourceConns {
		chunkerConn := chunkerConns[i]
		sourceConn := sourceConns[i]
		targetConn := targetConns[i]
		g.Go(func() error {
			err := ReadTables(
				ctx,
				chunkerConn,
				sourceConn,
				targetConn,
				shardingSpec,
				tableCh,
				cmd,
				batches,
			)
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
