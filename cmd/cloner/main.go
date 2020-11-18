package main

import (
	"github.com/alecthomas/kong"

	log "github.com/sirupsen/logrus"

	"cloner/pkg/clone"
)

var cli struct {
	clone.Globals

	Clone    clone.Clone    `cmd:"" help:"Clone database."`
	Checksum clone.Checksum `cmd:"" help:"Work In Progress! Checksum database."`
	Ping     clone.Ping     `cmd:"" help:"Ping the databases"`
}

func main() {
	ctx := kong.Parse(&cli)
	// Call the Run() method of the selected parsed command.
	err := ctx.Run(cli.Globals)
	if err != nil {
		log.Errorf("%+v", err)
	}
	ctx.FatalIfErrorf(err)
}
