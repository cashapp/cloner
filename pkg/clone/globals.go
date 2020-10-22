package clone

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	log "github.com/sirupsen/logrus"
)

type Globals struct {
	Source      DBConfig `help:"Database config of source to be copied from" prefix:"source-" embed:""`
	Target      DBConfig `help:"Database config of source to be copied from" prefix:"target-" embed:""`
	MetricsBind string   `help:"Bind address for Prometheus metrics." default:":9102"`
}

func (globals Globals) startMetricsServer() {
	go func() {
		log.Infof("Serving diagnostics on http://%s/metrics and http://%s/debug/pprof", globals.MetricsBind, globals.MetricsBind)
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(globals.MetricsBind, nil)
		log.Fatalf("%v", err)
	}()
}
