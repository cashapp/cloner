package clone

import (
	"context"
	"database/sql"
	"net"

	"github.com/pkg/errors"
	"google.golang.org/grpc"
	"vitess.io/vitess/go/vt/vitessdriver"
)

type DBConfig struct {
	Type DataSourceType `help:"Datasource name" enum:"mysql,vitess" optional:"" default:"mysql"`
	Host string         `help:"Hostname" optional:""`
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
	dialer := grpc.WithContextDialer(func(ctx context.Context, target string) (net.Conn, error) {
		raddr, err := net.ResolveUnixAddr("unixgram", "@egress.sock")
		if err != nil {
			return nil, err
		}
		d := &net.Dialer{}
		conn, err := d.DialContext(ctx, "unix", raddr.String())
		if err != nil {
			return nil, err
		}
		return conn, nil
	})
	return vitessdriver.OpenWithConfiguration(vitessdriver.Configuration{
		Address:         c.Host,
		Target:          "@replica",
		GRPCDialOptions: []grpc.DialOption{dialer},
	})
}

func (c DBConfig) openMySQL() (*sql.DB, error) {
	// TODO
	return nil, errors.Errorf("not implemented yet")
}
