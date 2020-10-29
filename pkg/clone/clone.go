package clone

import (
	"context"
	"database/sql"
	"fmt"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
)

type Clone struct {
	Consistent bool `help:"Clone at a specific GTID using consistent snapshot" default:"false"`

	QueueSize    int `help:"Queue size of the chunk queue" default:"10000"`
	ChunkSize    int `help:"Size of the chunks to diff" default:"1000"`
	ChunkerCount int `help:"Number of readers for chunks" default:"10"`

	ReaderCount int           `help:"Number of readers for diffing" default:"10"`
	ReadTimeout time.Duration `help:"Timeout for each read" default:"5m"`

	WriteBatchSize  int           `help:"Size of the write batches" default:"100"`
	WriterCount     int           `help:"Number of writers" default:"10"`
	WriteRetryCount int           `help:"Number of retries" default:"5"`
	WriteTimeout    time.Duration `help:"Timeout for each write" default:"30s"`

	CopySchema bool     `help:"Copy schema" default:"false"`
	Tables     []string `help:"Tables to clone (if unset will clone all of them)" optionals:""`
}

// Run applies the necessary changes to target to make it look like source
func (cmd *Clone) Run(globals Globals) error {
	globals.startMetricsServer()

	start := time.Now()

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

	// Load tables
	sourceVitessTarget, err := parseTarget(globals.Source.Database)
	if err != nil {
		return errors.WithStack(err)
	}
	tables, err := LoadTables(ctx, globals.Source.Type, sourceReader, sourceVitessTarget, cmd.Tables)
	if err != nil {
		return errors.WithStack(err)
	}

	writer, err := globals.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	writer.SetMaxOpenConns(cmd.WriterCount)
	// Refresh connections regularly so they don't go stale
	writer.SetConnMaxLifetime(time.Minute)

	// Create synced source and chunker conns
	var sourceConns []*sql.Conn
	if cmd.Consistent {
		sourceConns, err = OpenSyncedConnections(ctx, sourceReader, cmd.ChunkerCount+cmd.ReaderCount)
		if err != nil {
			return errors.WithStack(err)
		}
	} else {
		sourceConns, err = OpenConnections(ctx, sourceReader, cmd.ChunkerCount+cmd.ReaderCount)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	defer CloseConnections(sourceConns)

	chunkerConns := sourceConns[:cmd.ChunkerCount]
	sourceConns = sourceConns[cmd.ChunkerCount:]

	// Target reader
	// We can use a connection pool of unsynced connections for the target because the assumption is there are no
	// other writers to the target during the clone
	targetReader, err := globals.Target.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	targetReader.SetMaxOpenConns(cmd.ReaderCount)
	// Refresh connections regularly so they don't go stale
	targetReader.SetConnMaxLifetime(time.Minute)

	// Copy schema
	if cmd.CopySchema {
		err := CopySchema(ctx, tables, chunkerConns[0], target)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Parse the keyrange on the source so that we can filter the target
	var shardingSpec []*topodata.KeyRange
	if isSharded(sourceVitessTarget) {
		shardingSpec, err = key.ParseShardingSpec(sourceVitessTarget.Shard)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	// Start differs
	diffRequests := make(chan DiffRequest, cmd.QueueSize)
	for i, _ := range sourceConns {
		source := sourceConns[i]
		g.Go(func() error {
			return DiffChunks(ctx, source, targetReader, shardingSpec, cmd.ReadTimeout, diffRequests)
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
		err := readers(ctx, chunkerConns, tableCh, cmd, writer, diffRequests)
		// All tables are done, close the channels used to request work
		close(diffRequests)
		return errors.WithStack(err)
	})

	err = g.Wait()

	elapsed := time.Since(start)
	logger := log.WithField("duration", elapsed)
	if err != nil {
		if stackErr, ok := err.(stackTracer); ok {
			logger = logger.WithField("stacktrace", stackErr.StackTrace())
		}
		logger.WithError(err).Errorf("failed cloning %d tables: %+v", len(tables), err)
	} else {
		logger.Infof("done cloning %d tables", len(tables))
	}
	return errors.WithStack(err)
}

type stackTracer interface {
	StackTrace() errors.StackTrace
}

// readers runs all the readers in parallel and returns when they are all done
func readers(ctx context.Context, chunkerConns []*sql.Conn, tableCh chan *Table, cmd *Clone, writer *sql.DB, diffRequests chan DiffRequest) error {
	g, ctx := errgroup.WithContext(ctx)
	for i, _ := range chunkerConns {
		chunkerConn := chunkerConns[i]
		g.Go(func() error {
			err := ReadTables(ctx, chunkerConn, tableCh, cmd, writer, diffRequests)
			return errors.WithStack(err)
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
