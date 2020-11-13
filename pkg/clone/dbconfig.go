package clone

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"database/sql"
	"fmt"
	"io/ioutil"
	"net"
	"strings"

	"github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"gopkg.in/yaml.v2"
	"software.sslmate.com/src/go-pkcs12"
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
	GrpcCustomHeader []string       `help:"Custom GRPC headers separated by ="`
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
	})
}

func (c DBConfig) openMySQL() (*sql.DB, error) {
	return sql.Open("mysql", fmt.Sprintf("%s:%s@(%s)/%s?parseTime=true", c.Username, c.Password, c.Host, c.Database))
}

func (c DBConfig) openMisk() (*sql.DB, error) {
	config, err := parseConfig(c.MiskDatasource)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	for _, clusterConfig := range config.DataSourceClusters {
		return openMisk(clusterConfig.Writer)
	}
	return nil, errors.Errorf("No database found in %s: %v", c.MiskDatasource, config)
}

func (c DBConfig) String() string {
	if c.MiskDatasource != "" {
		return c.MiskDatasource
	} else {
		return fmt.Sprintf("%s/%s", c.Host, c.Database)
	}
}

type miskDataSourceConfig struct {
	Database                          string
	Type                              string
	Host                              string
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

func parseConfig(path string) (*miskDataSourceClustersConfig, error) {
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
	rootCAs := x509.NewCertPool()
	{
		if !strings.HasPrefix(c.TrustCertificateKeyStoreURL, "file://") {
			return nil, errors.Errorf("trust_certificate_key_store_url must be a file:// but is %q", c.TrustCertificateKeyStoreURL)
		}
		data, err := ioutil.ReadFile(c.TrustCertificateKeyStoreURL[7:])
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't read trust store from %q", c.TrustCertificateKeyStoreURL)
		}
		certificates, err := pkcs12.DecodeTrustStore(data, c.TrustCertificateKeyStorePassword)
		if err != nil {
			return nil, errors.Wrapf(err, "couldn't read trust store from %q", c.TrustCertificateKeyStoreURL)
		}
		for _, certificate := range certificates {
			rootCAs.AddCert(certificate)
		}
	}
	clientCerts := make([]tls.Certificate, 0, 1)
	{
		if strings.HasSuffix(c.ClientCertificateKeyStoreURL, ".p12") {
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

	err := mysql.RegisterTLSConfig("cloner", &tls.Config{
		RootCAs:      rootCAs,
		Certificates: clientCerts,
	})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	port := 3306
	if c.Type == "TIDB" {
		port = 4000
	}
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?collation=utf8mb4_unicode_ci&parseTime=true&tls=cloner",
		c.Username, c.Password, c.Host, port, c.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	return db, err
}
