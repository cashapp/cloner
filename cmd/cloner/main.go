package main

import (
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
	Clone     clone.Clone     `cmd:"" help:"Clone database."`
	Checksum  clone.Checksum  `cmd:"" help:"Work In Progress! Checksum database."`
	Replicate clone.Replicate `cmd:"" help:"Work In Progress! Checksum database."`
	Ping      clone.Ping      `cmd:"" help:"Ping the databases"`
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
