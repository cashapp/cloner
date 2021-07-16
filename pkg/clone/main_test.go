package clone

import (
	"os"
	"testing"

	log "github.com/sirupsen/logrus"
)

func TestMain(m *testing.M) {
	os.Exit(testMain(m))
}

func testMain(m *testing.M) int {
	log.SetLevel(log.DebugLevel)

	defer func() {
		//if vitessContainer != nil {
		//	vitessContainer.Close()
		//}
		//if tidbContainer != nil {
		//	tidbContainer.Close()
		//}
	}()

	// call flag.Parse() here if TestMain uses flags
	return m.Run()
}
