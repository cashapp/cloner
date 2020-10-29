package clone

import (
	"context"
	"os"
	"testing"

	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

// yep, globals, we should only have one container per test run
var vitessContainer *DatabaseContainer
var tidbContainer *DatabaseContainer

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	log.SetLevel(log.DebugLevel)

	defer func() {
		if vitessContainer != nil {
			vitessContainer.Close()
		}
		if tidbContainer != nil {
			tidbContainer.Close()
		}
	}()

	g, _ := errgroup.WithContext(context.Background())

	g.Go(func() error {
		var err error
		vitessContainer, err = startVitess()
		return errors.WithStack(err)
	})
	g.Go(func() error {
		var err error
		tidbContainer, err = startTidb()
		return errors.WithStack(err)
	})

	err := g.Wait()
	if err != nil {
		log.Panic(err)
	}

	// call flag.Parse() here if TestMain uses flags
	return m.Run()
}
