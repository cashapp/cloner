package clone

import (
	"context"
	"database/sql"
	"fmt"
	"net"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/vitessdriver"
)

type DBConfig struct {
	Type         DataSourceType `help:"Datasource name" enum:"mysql,vitess" optional:"" default:"mysql"`
	Host         string         `help:"Hostname" optional:""`
	EgressSocket string         `help:"Use an egress socket when connecting to Vitess, for example '@egress.sock'" optional:""`
	Username     string         `help:"User" optional:""`
	Password     string         `help:"Password" optional:""`
	Database     string         `help:"Database or Vitess shard with format <keyspace>/<shard>" optional:""`
}

type DataSourceType string

const (
	Vitess DataSourceType = "vitess"
	MySQL  DataSourceType = "mysql"
)

func (c DBConfig) DB() (*sql.DB, error) {
	if c.Type == Vitess {
		return c.openVitess()
	} else if c.Type == MySQL {
		return c.openMySQL()
	} else {
		return nil, errors.Errorf("not supported: %s", c.Type)
	}
}

func (c DBConfig) openVitess() (*sql.DB, error) {
	//logger := log.StandardLogger()
	//logger.SetLevel(log.DebugLevel)
	//grpclog.ReplaceGrpcLogger(log.NewEntry(logger))
	options := []grpc.DialOption{}
	if c.EgressSocket != "" {
		options = append(options,
			grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
				raddr, err := net.ResolveUnixAddr("unixgram", c.EgressSocket)
				if err != nil {
					return nil, err
				}
				d := &net.Dialer{}
				conn, err := d.DialContext(ctx, "unix", raddr.String())
				if err != nil {
					return nil, err
				}
				return conn, nil
			}))
	}
	return vitessdriver.OpenWithConfiguration(vitessdriver.Configuration{
		Address:         c.Host,
		Target:          c.Database,
		GRPCDialOptions: options,
	})
}

func (c DBConfig) openMySQL() (*sql.DB, error) {
	return sql.Open("mysql", fmt.Sprintf("%s:%s@(%s)/%s?parseTime=true", c.Username, c.Password, c.Host, c.Database))
}
