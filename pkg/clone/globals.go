package clone

import (
	"net/http"
	"os"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

type Globals struct {
	Source DBConfig `help:"Database config of source to be copied from" prefix:"source-" embed:""`
	Target DBConfig `help:"Database config of source to be copied from" prefix:"target-" embed:""`
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
