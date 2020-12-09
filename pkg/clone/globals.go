package clone

import (
	"time"
)

type SourceTargetConfig struct {
	Source DBConfig `help:"Database config of source to be copied from" prefix:"source-" embed:""`
	Target DBConfig `help:"Database config of source to be copied from" prefix:"target-" embed:""`
}

// ReaderConfig are used to control the read side, shared between Clone and Checksum
type ReaderConfig struct {
	SourceTargetConfig

	ChunkSize int `help:"Size of the chunks to diff" default:"5000"`

	QueueSize int `help:"Queue size of the chunk queue" default:"10000"`

	TableParallelism int           `help:"Number of tables to process concurrently" default:"10"`
	ReaderCount      int           `help:"Number of reader connections" default:"20"`
	ReadTimeout      time.Duration `help:"Timeout for faster reads like diffing a single chunk" default:"30s"`
	ReadRetries      uint64        `help:"How many times to retry reading a single chunk (with backoff)" default:"10"`

	Tables        []string `help:"Tables to checksum (if unset will clone all of them)" optional:"" name:"table"`
	IgnoreColumns []string `help:"Columns to ignore, format: \"table_name.column_name\"" optional:"" name:"ignore-column"`
}
