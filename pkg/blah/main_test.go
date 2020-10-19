package blah

import (
	"os"
	"testing"
)

var mainWasRun bool

func TestMain(m *testing.M) {
	mainWasRun = true
	os.Exit(m.Run())
}
