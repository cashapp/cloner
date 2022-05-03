package clone

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io"
	"io/ioutil"
	"net"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-mysql-org/go-mysql/replication"

	"github.com/go-sql-driver/mysql"
	"github.com/pavel-v-chernykh/keystore-go"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v2"
	"software.sslmate.com/src/go-pkcs12"
	"vitess.io/vitess/go/vt/key"
	"vitess.io/vitess/go/vt/proto/query"
	"vitess.io/vitess/go/vt/proto/topodata"
	"vitess.io/vitess/go/vt/topo/topoproto"
	"vitess.io/vitess/go/vt/vitessdriver"
)

type DBConfig struct {
	Type             DataSourceType `help:"Datasource name" enum:"mysql,vitess" optional:"" default:"mysql"`
	Host             string         `help:"Hostname" optional:""`
	EgressSocket     string         `help:"Use an egress socket when connecting to Vitess, for example '@egress.sock'" optional:""`
	Username         string         `help:"User" optional:""`
	Password         string         `help:"Password" optional:""`
	Database         string         `help:"Database or Vitess shard with format <keyspace>/<shard>" optional:""`
	MiskDatasource   string         `help:"Misk formatted config yaml file" optional:"" path:""`
	MiskReader       bool           `help:"Use the reader endpoint from Misk (defaults to writer)" optional:"" default:"false"`
	GrpcCustomHeader []string       `help:"Custom GRPC headers separated by ="`
	CA               string         `help:"CA root file, if this is specified then TLS will be enabled (PEM encoded)"`
	Cert             string         `help:"Certificate file for client side authentication (PEM encoded)"`
	Key              string         `help:"Key file for client side authentication (PEM encoded)"`
}

type DataSourceType string

const (
	Vitess DataSourceType = "vitess"
	MySQL  DataSourceType = "mysql"
)

func (c DBConfig) DB() (*sql.DB, error) {
	if c.MiskDatasource != "" {
		return c.openMisk()
	}
	if c.Type == Vitess {
		return c.openVitess(false)
	} else if c.Type == MySQL {
		return c.openMySQL()
	} else {
		return nil, errors.Errorf("not supported: %s", c.Type)
	}
}

func (c DBConfig) ReaderDB() (*sql.DB, error) {
	if c.Type == Vitess {
		return c.openVitess(true)
	} else {
		return c.DB()
	}
}

func (c DBConfig) openVitess(streaming bool) (*sql.DB, error) {
	//logger := log.StandardLogger()
	//logger.SetLevel(log.DebugLevel)
	//grpclog.ReplaceGrpcLogger(log.NewEntry(logger))
	var options []grpc.DialOption
	for _, customHeader := range c.GrpcCustomHeader {
		split := strings.Split(customHeader, "=")
		if len(split) != 2 {
			return nil, errors.Errorf("needs to be = separated key value pair: %s", customHeader)
		}
		key := split[0]
		value := split[1]
		options = append(options, grpc.WithUnaryInterceptor(func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = metadata.AppendToOutgoingContext(ctx, key, value)
			return invoker(ctx, method, req, reply, cc, opts...)
		}))
		options = append(options, grpc.WithStreamInterceptor(func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx = metadata.AppendToOutgoingContext(ctx, key, value)
			return streamer(ctx, desc, cc, method, opts...)
		}))
	}
	if c.EgressSocket != "" {
		options = append(options,
			grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
				raddr, err := net.ResolveUnixAddr("unixgram", c.EgressSocket)
				if err != nil {
					return nil, errors.WithStack(err)
				}
				d := &net.Dialer{}
				conn, err := d.DialContext(ctx, "unix", raddr.String())
				if err != nil {
					return nil, errors.WithStack(err)
				}
				return conn, nil
			}))
	}
	return vitessdriver.OpenWithConfiguration(vitessdriver.Configuration{
		Address:         c.Host,
		Target:          c.Database,
		Streaming:       streaming,
		GRPCDialOptions: options,
		DefaultLocation: "UTC",
	})
}

func (c DBConfig) openMySQL() (*sql.DB, error) {
	host := c.Host
	abstractSocket := false
	if strings.HasPrefix(host, "unix(@") {
		// The driver can't parse the dsn with an @ sign so we hack it
		abstractSocket = true
		// we remove the @ sign then put it back again
		host = strings.ReplaceAll(host, "@", "")
	}
	dsn := fmt.Sprintf("%s:%s@(%s)/%s?parseTime=true&loc=UTC", c.Username, c.Password, host, c.Database)
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if abstractSocket {
		cfg.Addr = "@" + cfg.Addr
	}
	tlsConfig, err := c.tlsConfig()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if tlsConfig != nil {
		err = mysql.RegisterTLSConfig("cloner", tlsConfig)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		cfg.TLSConfig = "cloner"
	}
	connector, err := mysql.NewConnector(cfg)
	return sql.OpenDB(connector), errors.WithStack(err)
}

func (c DBConfig) openMisk() (*sql.DB, error) {
	endpoint, err := c.miskEndpoint()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	// Let the database name from flags override if present
	// TODO We should probably be able to override everything with command line flags
	if c.Database != "" {
		endpoint.Database = c.Database
	}
	return openMisk(endpoint)
}

func (c DBConfig) miskEndpoint() (miskDataSourceConfig, error) {
	config, err := parseMiskDatasource(c.MiskDatasource)
	if err != nil {
		return miskDataSourceConfig{}, errors.WithStack(err)
	}
	for _, clusterConfig := range config.DataSourceClusters {
		if c.MiskReader {
			return clusterConfig.Reader, nil
		} else {
			return clusterConfig.Writer, nil
		}
	}
	return miskDataSourceConfig{}, errors.Errorf("No database found in %s: %v", c.MiskDatasource, config)
}

func (c DBConfig) String() string {
	if c.MiskDatasource != "" {
		return c.MiskDatasource
	} else {
		return fmt.Sprintf("%s/%s", c.Host, c.Database)
	}
}

func parseTarget(targetString string) (*query.Target, error) {
	// Default tablet type is master.
	target := &query.Target{
		TabletType: topodata.TabletType_MASTER,
	}
	last := strings.LastIndexAny(targetString, "@")
	if last != -1 {
		// No need to check the error. UNKNOWN will be returned on
		// error and it will fail downstream.
		tabletType, err := topoproto.ParseTabletType(targetString[last+1:])
		if err != nil {
			return target, err
		}
		target.TabletType = tabletType
		targetString = targetString[:last]
	}
	last = strings.LastIndexAny(targetString, "/:")
	if last != -1 {
		target.Shard = targetString[last+1:]
		targetString = targetString[:last]
	}
	target.Keyspace = targetString
	if target.Keyspace == "" {
		return target, fmt.Errorf("no keyspace in: %v", targetString)
	}
	if target.Shard == "" {
		return target, fmt.Errorf("no shard in: %v", targetString)
	}
	return target, nil
}

func (c DBConfig) Schema() (string, error) {
	if c.Type == Vitess {
		target, err := c.VitessTarget()
		if err != nil {
			return "", errors.WithStack(err)
		}
		if target != nil {
			return target.Keyspace, nil
		}
		schema := c.Database
		if schema == "" {
			return "", nil
		}
		// Remove the tablet type
		last := strings.LastIndexAny(c.Database, "@")
		if last == 0 {
			return "", nil
		}
		if last != -1 {
			schema = schema[0:last]
		}
		return schema, nil
	}
	if c.Database != "" {
		return c.Database, nil
	}
	if c.MiskDatasource != "" {
		endpoint, err := c.miskEndpoint()
		if err != nil {
			return "", errors.WithStack(err)
		}
		return endpoint.Database, nil
	}
	return "", nil
}

func (c DBConfig) ShardingKeyrange() ([]*topodata.KeyRange, error) {
	target, err := c.VitessTarget()
	if err != nil {
		return nil, errors.WithStack(err)
	}
	if target != nil {
		if target.Shard == "0" {
			// This is also used (incorrectly) to indicate an unsharded keyspace
			return nil, nil
		}
		return key.ParseShardingSpec(target.Shard)
	}
	return nil, nil
}

func (c DBConfig) IsSharded() (bool, error) {
	if c.Type == Vitess {
		return false, nil
	}
	target, err := c.VitessTarget()
	if err != nil {
		return false, errors.WithStack(err)
	}
	if target != nil {
		return isSharded(target), nil
	}
	return false, nil
}

func (c DBConfig) VitessTarget() (*query.Target, error) {
	if c.Type == Vitess && strings.Contains(c.Database, "/") {
		target, err := parseTarget(c.Database)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		return target, nil
	}
	return nil, nil
}

func (c DBConfig) BinlogSyncerConfig(serverID uint32) (replication.BinlogSyncerConfig, error) {
	if c.Type == Vitess {
		return replication.BinlogSyncerConfig{},
			errors.Errorf("can't stream binlogs from Vitess, you need to connect directly to underlying database")
	}
	if c.MiskDatasource != "" {
		endpoint, err := c.miskEndpoint()
		if err != nil {
			return replication.BinlogSyncerConfig{}, errors.WithStack(err)
		}

		var tlsConfig *tls.Config
		tlsConfig, err = miskTLSConfig(endpoint)
		if err != nil {
			return replication.BinlogSyncerConfig{}, errors.WithStack(err)
		}
		port := endpoint.Port
		if port == 0 {
			port = 3306
		}
		return replication.BinlogSyncerConfig{
			ServerID:                serverID,
			Flavor:                  "mysql",
			Host:                    endpoint.Host,
			Port:                    port,
			User:                    endpoint.Username,
			Password:                endpoint.Password,
			TLSConfig:               tlsConfig,
			TimestampStringLocation: time.UTC,
		}, nil
	} else {
		host, port, err := hostAndPort(c)
		if err != nil {
			return replication.BinlogSyncerConfig{}, errors.WithStack(err)
		}
		tlsConfig, err := c.tlsConfig()
		if err != nil {
			return replication.BinlogSyncerConfig{}, errors.WithStack(err)
		}
		return replication.BinlogSyncerConfig{
			ServerID:                serverID,
			Flavor:                  "mysql",
			Host:                    host,
			Port:                    port,
			User:                    c.Username,
			Password:                c.Password,
			TimestampStringLocation: time.UTC,
			TLSConfig:               tlsConfig,
		}, nil
	}
}

func (c DBConfig) tlsConfig() (*tls.Config, error) {
	if c.CA == "" {
		return nil, nil
	}

	caCert, err := ioutil.ReadFile(c.CA)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs: caCertPool,
	}

	if c.Key != "" {
		cert, err := tls.LoadX509KeyPair(c.Cert, c.Key)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	return tlsConfig, nil
}

func hostAndPort(c DBConfig) (string, uint16, error) {
	hostAndPort := strings.Split(c.Host, ":")
	if len(hostAndPort) == 1 {
		return hostAndPort[0], 3306, nil
	}
	host := hostAndPort[0]
	port, err := strconv.Atoi(hostAndPort[1])
	if err != nil {
		return "", 0, errors.WithStack(err)
	}
	return host, uint16(port), nil
}

type miskDataSourceConfig struct {
	Database                          string
	Type                              string
	Host                              string
	Port                              uint16
	Username                          string
	Password                          string
	TrustCertificateKeyStoreURL       string `yaml:"trust_certificate_key_store_url"`
	TrustCertificateKeyStorePassword  string `yaml:"trust_certificate_key_store_password"`
	ClientCertificateKeyStoreURL      string `yaml:"client_certificate_key_store_url"`
	ClientCertificateKeyStorePassword string `yaml:"client_certificate_key_store_password"`
}

type miskDataSourceClusterConfig struct {
	Reader miskDataSourceConfig
	Writer miskDataSourceConfig
}

type miskDataSourceClustersConfig struct {
	DataSourceClusters map[string]miskDataSourceClusterConfig `yaml:"data_source_clusters"`
}

func parseMiskDatasource(path string) (*miskDataSourceClustersConfig, error) {
	data, err := ioutil.ReadFile(path) // nolint: gosec
	if err != nil {
		return nil, errors.Wrapf(err, "could not open database configuration file %q", path)
	}
	config := &miskDataSourceClustersConfig{}
	err = yaml.Unmarshal(data, config)
	if err != nil {
		return nil, errors.Wrapf(err, "invalid configuration file %q", path)
	}
	return config, nil
}

func openMisk(c miskDataSourceConfig) (*sql.DB, error) {
	tlsConfig, err := miskTLSConfig(c)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = mysql.RegisterTLSConfig("cloner", tlsConfig)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	port := c.Port
	if port == 0 {
		port = 3306
		if c.Type == "TIDB" {
			port = 4000
		}
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?collation=utf8mb4_unicode_ci&parseTime=true&tls=cloner",
		c.Username, c.Password, c.Host, port, c.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return db, err
}

func miskTLSConfig(c miskDataSourceConfig) (*tls.Config, error) {
	rootCAs := x509.NewCertPool()
	{
		if !strings.HasPrefix(c.TrustCertificateKeyStoreURL, "file://") {
			return nil, errors.Errorf("trust_certificate_key_store_url must be a file:// but is %q", c.TrustCertificateKeyStoreURL)
		}
		data, err := ioutil.ReadFile(c.TrustCertificateKeyStoreURL[7:])
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't read trust store from %q", c.TrustCertificateKeyStoreURL)
		}
		var certificates []*x509.Certificate
		if strings.HasSuffix(c.TrustCertificateKeyStoreURL, ".jks") {
			certificates, err = loadCertsJKS(bytes.NewReader(data), []byte(c.TrustCertificateKeyStorePassword))
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't read trust store from %q", c.TrustCertificateKeyStoreURL)
			}
		} else {
			certificates, err = pkcs12.DecodeTrustStore(data, c.TrustCertificateKeyStorePassword)
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't read trust store from %q", c.TrustCertificateKeyStoreURL)
			}
		}
		for _, certificate := range certificates {
			rootCAs.AddCert(certificate)
		}
	}
	clientCerts := make([]tls.Certificate, 0, 1)
	{
		if c.ClientCertificateKeyStoreURL == "" {
			// No client cert
		} else if strings.HasSuffix(c.ClientCertificateKeyStoreURL, ".p12") {
			if !strings.HasPrefix(c.ClientCertificateKeyStoreURL, "file://") {
				return nil, errors.Errorf("client_certificate_key_store_url must be a file:// but is %q", c.ClientCertificateKeyStoreURL)
			}
			data, err := ioutil.ReadFile(c.ClientCertificateKeyStoreURL[7:])
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't read key store from %q", c.ClientCertificateKeyStoreURL)
			}
			key, cert, err := pkcs12.Decode(data, c.ClientCertificateKeyStorePassword)
			if err != nil {
				return nil, errors.Wrapf(err, "couldn't read key store from %q", c.ClientCertificateKeyStoreURL)
			}
			clientCert := tls.Certificate{
				Certificate: [][]byte{cert.Raw},
				PrivateKey:  key,
				Leaf:        cert,
			}
			clientCerts = append(clientCerts, clientCert)
		} else {
			return nil, errors.Errorf("can't read key format: %q", c.ClientCertificateKeyStoreURL)
		}
	}

	tlsConfig := &tls.Config{
		RootCAs:      rootCAs,
		Certificates: clientCerts,
		ServerName:   c.Host,
	}
	return tlsConfig, nil
}

func loadCertsJKS(r io.Reader, password []byte) ([]*x509.Certificate, error) {
	result := make([]*x509.Certificate, 0)
	ks, err := keystore.Decode(r, password)
	if err != nil {
		return nil, errors.Wrap(err, "failed to decode keystore")
	}

	keys := make([]string, 0, len(ks))

	for key := range ks {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	for _, key := range keys {
		store := ks[key]

		entry, ok := store.(*keystore.TrustedCertificateEntry)

		if !ok {
			return nil, errors.Errorf("cannot convert the store under %s to TrustedCertificateEntry", key)
		}

		if entry.Certificate.Type != "X.509" {
			return nil, errors.Errorf("expected an X.509 block but got %s", entry.Certificate.Type)
		}

		certificate, err := x509.ParseCertificate(entry.Certificate.Content)
		if err != nil {
			return nil, errors.WithStack(err)
		}
		result = append(result, certificate)
	}

	return result, nil
}
