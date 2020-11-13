package main

import (
	"github.com/alecthomas/kong"

	"cloner/pkg/clone"
	log "github.com/sirupsen/logrus"
)

var cli struct {
	clone.Globals

	Clone clone.Clone `cmd:"" help:"Clone database."`
	Ping  clone.Ping  `cmd:"" help:"Ping the databases"`
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
