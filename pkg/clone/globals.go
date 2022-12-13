package clone

import (
	"time"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"

	"github.com/BurntSushi/toml"
)

type SourceTargetConfig struct {
	Source DBConfig `help:"Database config of source to be copied from" prefix:"source-" embed:""`
	Target DBConfig `help:"Database config of source to be copied from" prefix:"target-" embed:""`

	IgnoreTables       []string `help:"Tables to ignore" optional:"true"`
	IgnoreTablePattern string   `help:"Regexp pattern of tables to ignore" optional:"true" default:"_cloner_.*"`
}

type TableConfig struct {
	IgnoreColumns  []string `toml:"ignore_columns" help:"Ignore columns in table"`
	TargetWhere    string   `toml:"target_where" help:"Extra where clause that is added on the target"`
	TargetHint     string   `toml:"target_hint" help:"Hint placed after the SELECT on target reads"`
	SourceWhere    string   `toml:"source_where" help:"Extra where clause that is added on the source"`
	SourceHint     string   `toml:"source_hint" help:"Hint placed after the SELECT on target reads"`
	ChunkSize      int      `toml:"chunk_size" help:"Global chunk size if chunk size not specified on the table"`
	WriteBatchSize int      `toml:"write_batch_size" help:"Global chunk size if chunk size not specified on the table"`
	WriteTimout    duration `toml:"write_timeout" help:"Global chunk size if chunk size not specified on the table"`
	KeyColumns     []string `toml:"keys" help:"Use these columns as a unique key for this table, defaults to primary key columns"`
}

type Config struct {
	Tables map[string]TableConfig `toml:"table"`
}

// ReaderConfig are used to control the read side, shared between Clone and Checksum
type ReaderConfig struct {
	SourceTargetConfig

	ChunkSize     int  `help:"Default size of the chunks to diff (can also be overridden per table)" default:"5000"`
	ShuffleChunks bool `help:"Process chunks in a random order, spreads out the write load but writing will be delayed because all chunks are read before writing starts" default:"false"`

	TableParallelism  int64         `help:"Number of tables to process concurrently" default:"10"`
	ReaderCount       int           `help:"Number of reader connections" default:"20"`
	ReaderParallelism int64         `help:"Number of reader goroutines" default:"200"`
	ReadTimeout       time.Duration `help:"Timeout for faster reads like diffing a single chunk" default:"30s"`
	ReadRetries       uint64        `help:"How many times to retry reading a single chunk (with backoff)" default:"10"`

	UseCRC32Checksum bool `help:"Compare chunks using CRC32 in the database before doing a full diff in memory" name:"use-crc32-checksum" default:"false"`

	UseConcurrencyLimits bool `help:"Use concurrency limits to automatically find the throughput of the underlying databases" default:"false"`

	ConfigFile string `help:"TOML formatted config file" short:"f" optional:"" type:"path"`

	// WriteBatchSize doesn't belong to ReaderConfig but we put that in the TableConfig when we load the table which is
	// code reused by both checksum and clone so it's easier to put this here for now
	WriteBatchSize int `help:"Default size of the write batch per transaction (can also be overridden per table)" default:"100"`

	FailedChunkRetryCount int `help:"Retry a chunk if it fails the checksum, this can be used to checksum across a replica with a master" default:"0"`

	Config Config `kong:"-"`
}

type WriterConfig struct {
	ReaderConfig

	WriteBatchStatementSize int           `help:"Size of the write batch per statement" default:"100"`
	WriterParallelism       int64         `help:"Number of writer goroutines" default:"200"`
	WriterCount             int           `help:"Number of writer connections" default:"10"`
	WriteRetries            uint64        `help:"Number of retries" default:"5"`
	WriteTimeout            time.Duration `help:"Timeout for each write" default:"30s"`

	SaveGTIDExecuted bool `help:"During replication save the gtid_executed into the checkpoint table, useful when reversing replication" default:"false"`

	NoDiff bool `help:"Clone without diffing using INSERT IGNORE can be faster as a first pass" default:"false"`
}

// LoadConfig loads the ConfigFile if specified
func (c *ReaderConfig) LoadConfig() error {
	if c.ConfigFile != "" {
		log.Infof("loading config from %v", c.ConfigFile)
		_, err := toml.DecodeFile(c.ConfigFile, &c.Config)
		if err != nil {
			return errors.WithStack(err)
		}
	}
	return nil
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}
