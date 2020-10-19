package main

import (
	"github.com/alecthomas/kong"

	"cloner/pkg/clone"
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
	ctx.FatalIfErrorf(err)
}
