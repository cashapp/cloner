package blah

import (
	"testing"
)

func TestNothing(t *testing.T) {
	if !mainWasRun {
		t.Error("TestMain was NOT run")
	}
}
