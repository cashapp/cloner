package clone

import (
	"context"
	_ "net/http/pprof"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"

	"github.com/pkg/errors"
)

var (
	eventsReceived = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "replication_events_received",
			Help: "How many events we've received",
		},
		[]string{"task"},
	)
	eventsProcessed = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "replication_events_processed",
			Help: "How many events we've processed",
		},
		[]string{"task"},
	)
	eventsIgnored = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "replication_events_ignored",
			Help: "How many events we've ignored",
		},
		[]string{"task"},
	)
	replicationLag = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "replication_lag",
			Help: "The time in seconds between a change applied to source is replicated to the target",
		},
		[]string{"task"},
	)
	replicationParallelism = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "replication_parallelism",
			Help: "How many serial sequences were we able to split a batch of transactions into",
		},
		[]string{"task"},
	)
	replicationParallelismBatchSize = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "replication_parallelism_batch_size",
			Help: "The actual size of a batch of transactions when they are applied",
		},
		[]string{"task"},
	)
	replicationParallelismApplyDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "replication_parallelism_apply_duration",
			Help:    "How long did it take do apply one batch of transactions",
			Buckets: defaultBuckets,
		},
		[]string{"task"},
	)
	heartbeatsRead = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "heartbeats_read",
			Help: "The number of times we've successfully read heartbeats",
		},
		[]string{"task"},
	)
	chunksSnapshotted = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "chunks_snapshotted",
			Help: "How many chunks has been read, partitioned by table.",
		},
		[]string{"task", "table"},
	)
)

func init() {
	prometheus.MustRegister(eventsReceived)
	prometheus.MustRegister(eventsProcessed)
	prometheus.MustRegister(eventsIgnored)
	prometheus.MustRegister(replicationLag)
	prometheus.MustRegister(heartbeatsRead)
	prometheus.MustRegister(chunksSnapshotted)
	prometheus.MustRegister(replicationParallelism)
	prometheus.MustRegister(replicationParallelismBatchSize)
	prometheus.MustRegister(replicationParallelismApplyDuration)
}

type Replicate struct {
	WriterConfig

	TaskName string `help:"The name of this task is used in heartbeat and checkpoints table as well as the name of the lease, only a single process can run as this task" default:"main"`
	ServerID uint32 `help:"Unique identifier of this server, defaults to a hash of the TaskName" optional:""`

	// TODO should this just be ReadParallelism
	ChunkParallelism int `help:"Number of chunks to snapshot concurrently" default:"10"` // the size of the channel buffer

	CheckpointTable      string        `help:"Name of the table to used on the target to save the current position in the replication stream" optional:"" default:"_cloner_checkpoint"`
	WatermarkTable       string        `help:"Name of the table to use to reconcile chunk result sets during snapshot rebuilds" optional:"" default:"_cloner_watermark"`
	HeartbeatTable       string        `help:"Name of the table to use for heartbeats which emits the real replication lag as the 'replication_lag_seconds' metric" optional:"" default:"_cloner_heartbeat"`
	SnapshotRequestTable string        `help:"Name of the table the user can requests snapshots from" optional:"" default:"_cloner_snapshot"`
	HeartbeatFrequency   time.Duration `help:"How often to write to the heartbeat table, this will be the resolution of the real replication lag metric, set to 0 if you want to disable heartbeats" default:"30s"`
	LogReplicationLag    bool          `help:"Log the replication lag after each heartbeat" default:"true"`
	CreateTables         bool          `help:"Create the required tables if they do not exist" default:"true"`
	ChunkBufferSize      int           `help:"Size of internal queues" default:"100"`
	ReconnectTimeout     time.Duration `help:"How long to try to reconnect after a replication failure (set to 0 to retry forever)" default:"5m"`

	DoSnapshot                               bool          `help:"Automatically starts a snapshot after running replication for 60s (configurable via --do-snapshot-delay)" default:"false"`
	DoSnapshotTables                         []string      `help:"Snapshot only these tables"`
	DoSnapshotMaxReplicationLag              time.Duration `help:"Start snapshot when replication lag drops below this" default:"10s"`
	DoSnapshotMaxReplicationLagCheckInterval time.Duration `help:"Maximum interval to check replication lag" default:"1m"`

	ReplicationParallelism          int           `help:"Many transactions to apply in parallel during replication" default:"1"`
	ParallelTransactionBatchMaxSize int           `help:"How large batch of transactions to parallelize" default:"100"`
	ParallelTransactionBatchTimeout time.Duration `help:"How long to wait for a batch of transactions to fill up before executing them anyway" default:"5s"`
	StartingGTID                    string        `help:"When starting a new replication this GTID set as the starting point" xor:"starting_gtid"`
	StartAtLastSourceGTID           bool          `help:"When starting a new replication use the value of the 'target_gtid' of the source checkpoint table" default:"false" xor:"starting_gtid"`
}

// Run replicates from source to target
func (cmd *Replicate) Run() error {
	var err error

	start := time.Now()

	err = cmd.ReaderConfig.LoadConfig()
	if err != nil {
		return errors.WithStack(err)
	}

	logrus.Infof("using config: %v", cmd)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	err = cmd.run(ctx)

	elapsed := time.Since(start)
	logger := logrus.WithField("duration", elapsed)
	if err != nil {
		if stackErr, ok := err.(stackTracer); ok {
			logger = logger.WithField("stacktrace", stackErr.StackTrace())
		}
		logger.WithError(err).Errorf("error: %+v", err)
	}

	return errors.WithStack(err)
}

func (cmd *Replicate) run(ctx context.Context) error {
	replicator, err := NewReplicator(*cmd)
	if err != nil {
		return errors.WithStack(err)
	}
	if cmd.DoSnapshot {
		go func() {
			logger := logrus.WithField("task", "snapshot")
			for {
				delay := cmd.DoSnapshotMaxReplicationLagCheckInterval
				lag := replicator.GetReplicationLag()
				if err != nil {
					logger.WithError(err).Warnf("failed to check replication lag, checking again in %v", delay)
					continue
				}
				if lag < cmd.DoSnapshotMaxReplicationLag {
					logger.Infof("replication lag %v below %v",
						lag, cmd.DoSnapshotMaxReplicationLag)
					break
				}

				delay = durationMin(lag, cmd.DoSnapshotMaxReplicationLagCheckInterval)

				logger.Infof("replication lag %v is still above %v, checking again in %v",
					lag, cmd.DoSnapshotMaxReplicationLag, delay)
				time.Sleep(delay)
			}
			err := replicator.snapshotter.start(ctx)
			if err != nil {
				logrus.Errorf("failed to snapshot: %v", err)
			}
		}()
	}

	return replicator.run(ctx)
}

func durationMin(a time.Duration, b time.Duration) time.Duration {
	if a.Microseconds() <= b.Microseconds() {
		return a
	} else {
		return b
	}
}

func (cmd *Replicate) ReconnectBackoff() backoff.BackOff {
	b := backoff.NewExponentialBackOff()
	// We try to re-connect for this amount of time before we give up
	// on Kubernetes that generally means we will get restarted with a backoff
	b.MaxElapsedTime = cmd.ReconnectTimeout
	return b
}

// Replicator replicates from source to target
type Replicator struct {
	config Replicate

	snapshotter         *Snapshotter
	heartbeat           *Heartbeat
	transactionStreamer *TransactionStream
	transactionWriter   *TransactionWriter
}

func NewReplicator(config Replicate) (*Replicator, error) {
	var err error
	r := Replicator{
		config: config,
	}

	r.snapshotter, err = NewSnapshotter(r.config)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.heartbeat, err = NewHeartbeat(r.config)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.transactionWriter, err = NewTransactionWriter(r.config)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	r.transactionStreamer, err = NewTransactionStreamer(r.config)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return &r, nil
}

func (r *Replicator) run(ctx context.Context) error {
	err := r.init(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	g, ctx := errgroup.WithContext(ctx)

	transactions := make(chan Transaction, r.config.ReplicationParallelism)
	g.Go(RestartLoop(ctx, r.config.ReconnectBackoff(), func(b backoff.BackOff) error {
		return r.transactionStreamer.Run(ctx, b, transactions)
	}))

	transactionsAfterSnapshot := make(chan Transaction, r.config.ReplicationParallelism)
	g.Go(RestartLoop(ctx, r.config.ReconnectBackoff(), func(b backoff.BackOff) error {
		return r.snapshotter.Run(ctx, b, transactions, transactionsAfterSnapshot)
	}))

	g.Go(RestartLoop(ctx, r.config.ReconnectBackoff(), func(b backoff.BackOff) error {
		return r.transactionWriter.Run(ctx, b, transactionsAfterSnapshot)
	}))

	if r.config.HeartbeatFrequency > 0 {
		g.Go(RestartLoop(ctx, r.config.ReconnectBackoff(), func(b backoff.BackOff) error {
			return r.heartbeat.Run(ctx, b)
		}))
	}
	err = g.Wait()
	if err != nil {
		return errors.WithStack(err)
	}
	return err
}

func RestartLoop(ctx context.Context, b backoff.BackOff, loop func(b backoff.BackOff) error) func() error {
	return func() error {
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default: // yyuan TODO: check if this goes back to the loop
			}
			err := loop(b)
			if errors.Is(err, context.Canceled) {
				return errors.WithStack(err)
			}
			logrus.WithError(err).Errorf("replication write loop failed, restarting: %+v", err)
			sleepTime := b.NextBackOff()
			if sleepTime == backoff.Stop {
				return errors.Wrapf(err, "failed to reconnect after retries")
			}
			select {
			case <-time.After(sleepTime):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

func (r *Replicator) init(ctx context.Context) error {
	err := r.snapshotter.Init(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.heartbeat.Init(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.transactionWriter.Init(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	err = r.transactionStreamer.Init(ctx)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (r *Replicator) GetReplicationLag() time.Duration {
	return r.heartbeat.getReplicationLag()
}
