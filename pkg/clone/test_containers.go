package clone

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// yep, globals, we should only have one container per test run
var vitessContainer *DatabaseContainer
var tidbContainer *DatabaseContainer

func createSchema(config DBConfig, database string) error {
	db, err := config.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	if config.Type != Vitess {
		_, err = db.Exec("CREATE DATABASE " + database)
		if err != nil {
			return errors.WithStack(err)
		}
	}

	config.Database = database
	db, err = config.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = db.Exec(`
		CREATE TABLE customers (
		  id BIGINT(20) NOT NULL AUTO_INCREMENT,
		  name VARCHAR(255) NOT NULL,
		  PRIMARY KEY (id)
		)
	`)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = db.Exec(`
		CREATE TABLE transactions (
		  id BIGINT(20) NOT NULL AUTO_INCREMENT,
		  customer_id BIGINT(20) NOT NULL,
		  amount_cents INTEGER NOT NULL,
		  description VARCHAR(255) NOT NULL,
		  PRIMARY KEY (customer_id, id)
		) ENGINE=MyISAM DEFAULT CHARSET=utf8mb4;
	`)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func createMysqlSchema(config DBConfig, database string) error {
	db, err := config.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = db.Exec("CREATE DATABASE " + database)
	if err != nil {
		return errors.WithStack(err)
	}

	config.Database = database
	db, err = config.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = db.Exec(`
		CREATE TABLE customers (
		  id BIGINT(20) NOT NULL AUTO_INCREMENT,
		  name VARCHAR(255) NOT NULL,
		  PRIMARY KEY (id)
		)
	`)
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = db.Exec(`
		CREATE TABLE transactions (
		  id BIGINT(20) NOT NULL AUTO_INCREMENT,
		  customer_id BIGINT(20) NOT NULL,
		  amount_cents INTEGER NOT NULL,
		  description VARCHAR(255) NOT NULL,
		  PRIMARY KEY (id)
		) DEFAULT CHARSET=utf8mb4;
	`)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

type DatabaseContainer struct {
	pool     *dockertest.Pool
	cleanups []func()
	resource *dockertest.Resource
	config   DBConfig
}

func (c *DatabaseContainer) Close() error {
	return c.pool.Purge(c.resource)
}

func (c *DatabaseContainer) Config() DBConfig {
	return c.config
}

func startVitess() error {
	if vitessContainer != nil {
		err := resetData(vitessContainer.config)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	}

	log.Debugf("starting Vitess")
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		return errors.WithStack(err)
	}
	pool.MaxWait = 20 * time.Second // Things are kinda slow on GitHub Actions

	// pulls an image, creates a container based on it and runs it
	path, err := os.Getwd()
	if err != nil {
		return errors.WithStack(err)
	}
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "vitess/base",
		Tag:        "v15.0.0",
		Cmd: []string{
			"vttestserver",
			"--port=15000",
			"--mysql_bind_host=0.0.0.0",
			"--keyspaces=main,customer",
			"--data_dir=/vt/vtdataroot",
			"--schema_dir=schema",
			"--num_shards=1,2",
		},
		ExposedPorts: []string{"15000", "15001", "15002", "15003"},
		Mounts: []string{
			path + "/../../test/schema:/vt/src/vitess.io/vitess/schema",
		},
	})
	if err != nil {
		return errors.WithStack(err)
	}
	err = resource.Expire(15 * 60)
	if err != nil {
		_ = pool.Purge(resource)
		return errors.WithStack(err)
	}

	config := DBConfig{
		Type:     Vitess,
		Host:     "localhost:" + resource.GetPort("15001/tcp"),
		Username: "vt_dba",
		Password: "",
		Database: "customer@master",
	}

	// exponential backoff-Retry, because the application in the container might not be ready to accept connections yet
	time.Sleep(1 * time.Second)
	if err := pool.Retry(func() error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var err error
		db, err := config.DB()
		if err != nil {
			return errors.WithStack(err)
		}
		err = db.PingContext(ctx)
		if err != nil {
			fmt.Printf("%+v\n", err)
			return errors.WithStack(err)
		}
		conn, err := db.Conn(ctx)
		if err != nil {
			fmt.Printf("%+v\n", err)
			return errors.WithStack(err)
		}
		rows, err := conn.QueryContext(ctx, "SELECT * FROM customers")
		if err != nil {
			fmt.Printf("%+v\n", err)
			return errors.WithStack(err)
		}
		defer rows.Close()
		rows.Next()
		return nil
	}); err != nil {
		_ = pool.Purge(resource)
		return errors.WithStack(err)
	}

	// Allow external access to the underlying mysql database
	exitCode, err := resource.Exec(
		[]string{
			"mysql",
			"-S", "/vt/vtdataroot/vt_0000000001/mysql.sock",
			"-u", "root",
			"mysql",
			"-e",
			"grant all on *.* to 'vt_dba'@'%'; grant REPLICATION CLIENT, REPLICATION SLAVE on *.* to 'vt_app'@'%'",
		},
		dockertest.ExecOptions{StdOut: os.Stdout, StdErr: os.Stderr},
	)
	if err != nil {
		return errors.WithStack(err)
	}
	if exitCode != 0 {
		_ = pool.Purge(resource)
		return errors.Errorf("non-zero exit code: %d", exitCode)
	}

	vitessContainer = &DatabaseContainer{pool: pool, resource: resource, config: config}
	vitessContainer.cleanups = append(vitessContainer.cleanups, func() {
		_ = resource.Close()
	})

	return nil
}

func resetData(config DBConfig) error {
	err := deleteAllData(config)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}

func deleteAllData(config DBConfig) error {
	db, err := config.DB()
	if err != nil {
		return errors.WithStack(err)
	}
	defer db.Close()

	_, err = db.Exec("DELETE FROM customers")
	if err != nil {
		return errors.WithStack(err)
	}
	_, err = db.Exec("DELETE FROM transactions")
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func startTidb() error {
	if tidbContainer != nil {
		err := resetData(tidbContainer.config)
		if err != nil {
			return errors.WithStack(err)
		}
		return nil
	}

	log.Debugf("starting TiDB")
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("pingcap/tidb", "v5.0.0-nightly", []string{})
	if err != nil {
		return errors.WithStack(err)
	}
	err = resource.Expire(15 * 60)
	if err != nil {
		_ = pool.Purge(resource)
		return errors.WithStack(err)
	}

	config := DBConfig{
		Type:     MySQL,
		Host:     "localhost:" + resource.GetPort("4000/tcp"),
		Username: "root",
		Password: "",
		Database: "mysql",
	}

	// exponential backoff-Retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err := config.DB()
		if err != nil {
			return errors.WithStack(err)
		}
		return db.Ping()
	}); err != nil {
		_ = pool.Purge(resource)
		return errors.WithStack(err)
	}

	err = createSchema(config, "mydatabase")
	if err != nil {
		_ = pool.Purge(resource)
		return errors.WithStack(err)
	}

	config.Database = "mydatabase"

	tidbContainer = &DatabaseContainer{pool: pool, resource: resource, config: config}
	tidbContainer.cleanups = append(tidbContainer.cleanups, func() {
		_ = resource.Close()
	})

	return nil
}

func startMysql() (*DatabaseContainer, error) {
	log.Debugf("starting MySQL")
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	serverID := rand.Intn(10_000_000)
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "mysql",
		Tag:        "5.7",
		Platform:   "linux/x86_64",
		Cmd: []string{
			"--log-bin=/tmp/binlog",
			"--gtid_mode=ON",
			"--enforce-gtid-consistency=ON",
			fmt.Sprintf("--server-id=%v", serverID),
		},
		Env: []string{"MYSQL_ROOT_PASSWORD=secret"}})
	if err != nil {
		return nil, errors.WithStack(err)
	}
	err = resource.Expire(15 * 60)
	if err != nil {
		_ = pool.Purge(resource)
		return nil, errors.WithStack(err)
	}

	config := DBConfig{
		Type:     MySQL,
		Host:     "localhost:" + resource.GetPort("3306/tcp"),
		Username: "root",
		Password: "secret",
		Database: "mysql",
	}

	// exponential backoff-Retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		var err error
		db, err := config.DB()
		if err != nil {
			return errors.WithStack(err)
		}
		return db.Ping()
	}); err != nil {
		_ = pool.Purge(resource)
		return nil, errors.WithStack(err)
	}

	err = createMysqlSchema(config, "mydatabase")
	if err != nil {
		_ = pool.Purge(resource)
		return nil, errors.WithStack(err)
	}

	config.Database = "mydatabase"

	container := &DatabaseContainer{pool: pool, resource: resource, config: config}
	container.cleanups = append(container.cleanups, func() {
		_ = resource.Close()
	})
	return container, nil
}

// startAll (re)starts both Vitess and TiDB in parallel
func startAll() (*DatabaseContainer, *DatabaseContainer, error) {
	g, _ := errgroup.WithContext(context.Background())
	g.Go(startVitess)
	g.Go(startTidb)
	err := g.Wait()
	return vitessContainer, tidbContainer, err
}
