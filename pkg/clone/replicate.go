package clone

import (
	"context"
	"github.com/cenkalti/backoff/v4"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
	"net/http"
	_ "net/http/pprof"
	"time"

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
			Help: "The time in milliseconds between a change applied to source is replicated to the target",
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

	TaskName string `help:"The name of this task is used in heartbeat and checkpoints table as well as the name of the lease, only a single process can run as this task" required:""`
	ServerID uint32 `help:"Unique identifier of this server, defaults to a hash of the TaskName" optional:""`

	ChunkParallelism int `help:"Number of chunks to snapshot concurrently" default:"10"`

	CheckpointTable    string        `help:"Name of the table to used on the target to save the current position in the replication stream" optional:"" default:"_cloner_checkpoint"`
	WatermarkTable     string        `help:"Name of the table to use to reconcile chunk result sets during snapshot rebuilds" optional:"" default:"_cloner_watermark"`
	HeartbeatTable     string        `help:"Name of the table to use for heartbeats which emits the real replication lag as the 'replication_lag_seconds' metric" optional:"" default:"_cloner_heartbeat"`
	HeartbeatFrequency time.Duration `help:"How often to to write to the heartbeat table, this will be the resolution of the real replication lag metric, set to 0 if you want to disable heartbeats" default:"30s"`
	CreateTables       bool          `help:"Create the required tables if they do not exist" default:"true"`
	ChunkBufferSize    int           `help:"Size of internal queues" default:"100"`
	ReconnectTimeout   time.Duration `help:"How long to try to reconnect after a replication failure (set to 0 to retry forever)" default:"5m"`

	ReplicationParallelism          int64         `help:"Many transactions to apply in parallel during replication" default:"1"`
	ParallelTransactionBatchMaxSize int           `help:"How large batch of transactions to parallelize" default:"100"`
	ParallelTransactionBatchTimeout time.Duration `help:"How long to wait for a batch of transactions to fill up before executing them anyway" default:"5s"`
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
	http.HandleFunc("/snapshot", func(writer http.ResponseWriter, request *http.Request) {
		go func() {
			err := replicator.snapshot(context.Background())
			if err != nil {
				logrus.Errorf("failed to snapshot: %v", err)
			}
		}()
		// TODO return status/errors back to the caller?
		_, _ = writer.Write([]byte(""))
	})

	return replicator.run(ctx)
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

	transactions := make(chan Transaction)
	g.Go(RestartLoop(ctx, r.config.ReconnectBackoff(), func(b backoff.BackOff) error {
		return r.transactionStreamer.Run(ctx, b, transactions)
	}))

	transactionsAfterSnapshot := make(chan Transaction)
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
			default:
			}
			err := loop(b)
			if errors.Is(err, context.Canceled) {
				return errors.WithStack(err)
			}
			logrus.WithError(err).Errorf("replication write loop failed, restarting")
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

func (r *Replicator) snapshot(ctx context.Context) error {
	return r.snapshotter.snapshot(ctx)
}
