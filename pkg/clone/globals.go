package clone

import (
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

type Globals struct {
	Source DBConfig `help:"Database config of source to be copied from" prefix:"source-" embed:""`
	Target DBConfig `help:"Database config of source to be copied from" prefix:"target-" embed:""`
}

// ReaderConfig are used to control the read side, shared between Clone and Checksum
type ReaderConfig struct {
	ChunkSize       int           `help:"Size of the chunks to diff" default:"10000"`
	ChunkingTimeout time.Duration `help:"Timeout for the chunking (which can take a really long time)" default:"1h"`
	ChunkerCount    int           `help:"Number of readers for chunks" default:"10"`

	QueueSize int `help:"Queue size of the chunk queue" default:"10000"`

	TableParallelism int           `help:"Number of tables to process concurrently" default:"10"`
	ReaderCount      int           `help:"Number of reader connections" default:"20"`
	ReadTimeout      time.Duration `help:"Timeout for faster reads like diffing a single chunk" default:"30s"`
	ReadRetries      uint64        `help:"How many times to retry reading a single chunk (with backoff)" default:"10"`

	Tables []string `help:"Tables to checksum (if unset will clone all of them)" optional:""`
}

func inKubernetes() bool {
	return os.Getenv("KUBERNETES_PORT") != ""
}

func (globals Globals) startMetricsServer() {
	go func() {
		bindAddr := "localhost:9102"
		if inKubernetes() {
			bindAddr = ":9102"
		}
		log.Infof("Serving diagnostics on http://%s/metrics and http://%s/debug/pprof", bindAddr, bindAddr)
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(bindAddr, nil)
		log.Fatalf("%v", err)
	}()
}
