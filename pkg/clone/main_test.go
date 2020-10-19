package clone

import (
	"os"
	"sync"
	"testing"

	log "github.com/sirupsen/logrus"
)

// yep, globals, we should only have one container per test run
var vitessContainer *DatabaseContainer
var mysqlContainer *DatabaseContainer

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	log.SetLevel(log.DebugLevel)

	defer func() {
		if vitessContainer != nil {
			vitessContainer.Close()
		}
		if mysqlContainer != nil {
			mysqlContainer.Close()
		}
	}()

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		vitessContainer, err = startVitess()
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		var err error
		mysqlContainer, err = startMysql()
		if err != nil {
			log.Fatalf("%+v", err)
		}
	}()

	wg.Wait()

	// call flag.Parse() here if TestMain uses flags
	return m.Run()
}
