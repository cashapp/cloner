package clone

import (
	"database/sql"
)

type Config struct {
	QueueSize      int `help:"Queue size of the chunk queue" default:"1000"`
	ChunkSize      int `help:"Size of the chunks to diff" default:"1000"`
	WriteBatchSize int `help:"Size of the write batches" default:"100"`
	ChunkerCount   int `help:"Number of readers for chunks" default:"10"`
	ReaderCount    int `help:"Number of readers for diffing" default:"10"`
	WriterCount    int `help:"Number of writers" default:"10"`
}

func (cmd *Config) Run(globals Globals) error {
	// TODO open the databases
	var from *sql.DB
	var to *sql.DB
	return Clone(cmd, from, to)
}
