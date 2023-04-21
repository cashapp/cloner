package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"os/user"
	"path"
	"strings"
)

func main() {
	usr, _ := user.Current()
	base := path.Join(usr.HomeDir, "Development")

	InDir(path.Join(base, "go-tpc"), func() {
		Exec("./bin/go", "build", "-o", "go-tpc", "./cmd/go-tpc")
	})
	InDir(path.Join(base, "cloner"), func() {
		Exec("./bin/go", "build", "-o", "cloner", "./cmd/cloner")
	})

	ExecIgnoreErr("mysql", "-u", "root", "-e", "drop database tpcc;")
	ExecIgnoreErr("mysql", "-u", "root", "-e", "drop database tpcc_clone;")
	Exec("mysql", "-u", "root", "-e", "create database tpcc;")
	Exec("mysql", "-u", "root", "-e", "create database tpcc_clone;")

	tpc := path.Join(base, "go-tpc", "go-tpc")
	cloner := path.Join(base, "cloner", "cloner")

	Exec(tpc,
		"-H", "127.0.0.1",
		"-P", "3306",
		"-D", "tpcc",
		"tpcc",
		"--warehouses", "4",
		"prepare",
		"-T", "4",
		"--no-check")
	Exec(tpc,
		"-H", "127.0.0.1",
		"-P", "3306",
		"-D", "tpcc",
		"tpcc",
		"--warehouses", "4",
		"--time", "1m",
		"run",
		"-T", "10")
	Exec(cloner,
		"clone",
		"--source-host", "127.0.0.1",
		"--source-database", "tpcc",
		"--source-username", "root",

		"--target-host", "127.0.0.1",
		"--target-database", "tpcc_clone",
		"--target-username", "root",

		"--table-parallelism", "6",
		"--writer-parallelism", "10",
		"--reader-parallelism", "30",
		"--read-timeout", "120s",
		"--chunk-size", "5000",
		"--read-retries", "10",
		"--write-retries", "10",
		"--write-batch-size", "100",
		"--write-timeout", "60s",
		"--ignore-tables", "history",
		"--copy-schema")

	// Start replicating and snapshotting
	replicate := exec.Command(cloner,
		"replicate",
		"--source-host", "127.0.0.1",
		"--source-database", "tpcc",
		"--source-username", "root",

		"--target-host", "127.0.0.1",
		"--target-database", "tpcc_clone",
		"--target-username", "root",

		"--table-parallelism", "6",
		"--read-timeout", "120s",
		"--chunk-size", "10000",
		"--read-retries", "10",
		"--write-retries", "10",
		"--write-batch-size", "100",
		"--write-timeout", "60s",
		"--chunk-parallelism", "50",
		"--parallel-transaction-batch-max-size", "2000",
		"--heartbeat-frequency", "10s",
		"--do-snapshot-max-replication-lag-check-interval", "10s",
		"--replication-parallelism", "1",
		"--save-gtid-executed",
		"--ignore-tables", "history",
		"--do-snapshot")
	replicate.Stdout = os.Stdout
	startSnapshot := OnFirstOutput(os.Stderr, "starting snapshot", func() {
		go func() {
			// We run a burst of high load writes while the snapshot runs
			Exec(tpc,
				"-H", "127.0.0.1",
				"-P", "3306",
				"-D", "tpcc",
				"tpcc",
				"--warehouses", "4",
				"--time", "1m",
				"run",
				"-T", "4")
		}()
	})
	snapshotDone := OnFirstOutput(startSnapshot, "snapshot done", func() {
		go func() {
			checksum := exec.Command(cloner,
				"checksum",
				"--metrics-port", "9103",
				"--source-host", "127.0.0.1",
				"--source-database", "tpcc",
				"--source-username", "root",

				"--target-host", "127.0.0.1",
				"--target-database", "tpcc_clone",
				"--target-username", "root",

				"--table-parallelism", "6",
				"--reader-parallelism", "20",
				"--reader-count", "50",
				"--read-timeout", "120s",
				"--chunk-size", "5000",
				"--ignore-tables", "history",
				"--failed-chunk-retry-count", "10",
				"--read-retries", "10")
			checksum.Stdout = os.Stdout
			checksum.Stderr = OnFirstOutput(os.Stderr, "no diffs found", func() {
				fmt.Println("test successful!")
				os.Exit(0)
			})
			Run(checksum)
		}()
	})
	replicate.Stderr = snapshotDone
	Run(replicate)
}

func Run(cmd *exec.Cmd) {
	fmt.Println(strings.Join(cmd.Args, " "))
	NoErr(cmd.Run())
}

func RunIgnoreErr(cmd *exec.Cmd) {
	fmt.Println(strings.Join(cmd.Args, " "))
	err := cmd.Run()
	var exitError *exec.ExitError
	if errors.As(err, &exitError) {
		return
	}
	NoErr(err)
}

func OnFirstOutput(output io.Writer, substr string, f func()) io.Writer {
	pr, pw := io.Pipe()
	tee := io.TeeReader(pr, output)
	go func() {
		s := bufio.NewScanner(tee)
		for s.Scan() {
			if strings.Contains(s.Text(), substr) {
				f()
				break
			}
			NoErr(s.Err())
		}
		// Read everything else (TeeReader won't forward unless you read)
		for s.Scan() {
		}
	}()
	return pw
}

func Exec(name string, arg ...string) {
	cmd := exec.Command(name, arg...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	Run(cmd)
}

func ExecIgnoreErr(name string, arg ...string) {
	fmt.Print(name)
	fmt.Print(" ")
	fmt.Println(strings.Join(arg, " "))
	cmd := exec.Command(name, arg...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	RunIgnoreErr(cmd)
}

func InDir(dir string, f func()) {
	prev, err := os.Getwd()
	NoErr(err)
	err = os.Chdir(dir)
	defer func() {
		os.Chdir(prev)
	}()
	f()
}

func NoErr(err error) {
	if err != nil {
		panic(err)
	}
}
