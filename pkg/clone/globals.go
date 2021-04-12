package clone

import (
	"github.com/pkg/errors"
	"time"

	"github.com/BurntSushi/toml"
)

type SourceTargetConfig struct {
	Source DBConfig `help:"Database config of source to be copied from" prefix:"source-" embed:""`
	Target DBConfig `help:"Database config of source to be copied from" prefix:"target-" embed:""`
}

type TableConfig struct {
	IgnoreColumns []string `toml:"ignore_columns" help:"Ignore columns in table"`
	TargetWhere   string   `toml:"target_where" help:"Extra where clause that is added on the target"`
	SourceWhere   string   `toml:"source_where" help:"Extra where clause that is added on the source"`
}

type Config struct {
	Tables map[string]TableConfig `toml:"table"`
}

// ReaderConfig are used to control the read side, shared between Clone and Checksum
type ReaderConfig struct {
	SourceTargetConfig

	ChunkSize int `help:"Size of the chunks to diff" default:"5000"`

	TableParallelism int           `help:"Number of tables to process concurrently" default:"10"`
	ReaderCount      int           `help:"Number of reader connections" default:"20"`
	ReadTimeout      time.Duration `help:"Timeout for faster reads like diffing a single chunk" default:"30s"`
	ReadRetries      uint64        `help:"How many times to retry reading a single chunk (with backoff)" default:"10"`

	ConfigFile string `help:"TOML formatted config file" short:"f" optional:"" type:"path"`

	Config Config `kong:"-"`
}

// LoadConfig loads the ConfigFile if specified
func (c *ReaderConfig) LoadConfig() error {
	if c.ConfigFile == "" {
		return nil
	}
	_, err := toml.DecodeFile(c.ConfigFile, &c.Config)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
