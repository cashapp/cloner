package main

import (
	"net/http"
	"os"

	"github.com/alecthomas/kong"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "github.com/sirupsen/logrus"

	"cloner/pkg/clone"
)

var cli struct {
	Clone    clone.Clone    `cmd:"" help:"Clone database."`
	Checksum clone.Checksum `cmd:"" help:"Work In Progress! Checksum database."`
	Ping     clone.Ping     `cmd:"" help:"Ping the databases"`
}

func inKubernetes() bool {
	return os.Getenv("KUBERNETES_PORT") != ""
}

func startMetricsServer() {
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

func main() {
	ctx := kong.Parse(&cli)

	startMetricsServer()

	// Call the Run() method of the selected parsed command.
	err := ctx.Run()
	if err != nil {
		log.Errorf("%+v", err)
	}
	ctx.FatalIfErrorf(err)
}
