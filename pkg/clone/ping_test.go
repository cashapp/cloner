package clone

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPing(t *testing.T) {
	ping := &Ping{
		Table: "customers",
	}
	err := ping.Run(Globals{
		Source: vitessContainer.Config(),
		Target: tidbContainer.Config(),
	})
	assert.NoError(t, err)
}
