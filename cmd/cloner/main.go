package main

import (
	"fmt"
	"net/http"
	"os"

	"github.com/alecthomas/kong"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	log "github.com/sirupsen/logrus"

	"cloner/pkg/clone"
)

// TimestampFormat for Cash Platform.
const TimestampFormat = `2006-01-02T15:04:05.000`

var cli struct {
	Clone     clone.Clone     `cmd:"" help:"Best effort copy of databases"`
	Checksum  clone.Checksum  `cmd:"" help:"Find differences between databases"`
	Replicate clone.Replicate `cmd:"" help:"Replicate from one database to another and consistent clone"`
	Ping      clone.Ping      `cmd:"" help:"Ping the databases to check the config is right"`

	MetricsPort int `help:"Which port to publish metrics and debugging info to" default:"9102"`
}

func inKubernetes() bool {
	return os.Getenv("KUBERNETES_PORT") != ""
}

func startMetricsServer() {
	go func() {
		bindAddr := fmt.Sprintf("localhost:%d", cli.MetricsPort)
		if inKubernetes() {
			bindAddr = fmt.Sprintf(":%d", cli.MetricsPort)
		}
		log.Infof("Serving diagnostics on http://%s/metrics and http://%s/debug/pprof", bindAddr, bindAddr)
		http.Handle("/metrics", promhttp.Handler())
		err := http.ListenAndServe(bindAddr, nil)
		log.Fatalf("%v", err)
	}()
}

type utcFormatter struct {
	log.Formatter
}

func (u utcFormatter) Format(e *log.Entry) ([]byte, error) {
	e.Time = e.Time.UTC()
	return u.Formatter.Format(e)
}

func setupLogFormat() {
	// NOTE: We might want to consider setting "DataKey":true to prevent collisions.
	jsonFormatter := &log.JSONFormatter{
		FieldMap: log.FieldMap{
			log.FieldKeyMsg:  "message",
			log.FieldKeyTime: "timestamp",
		},
	}
	jsonFormatter.TimestampFormat = TimestampFormat
	formatter := &utcFormatter{jsonFormatter}
	log.SetFormatter(formatter)
}

func main() {
	ctx := kong.Parse(&cli)

	startMetricsServer()
	setupLogFormat()

	// Call the Run() method of the selected parsed command.
	err := ctx.Run()
	if err != nil {
		log.Errorf("%+v", err)
	}
	ctx.FatalIfErrorf(err)
}
