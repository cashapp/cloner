package clone

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func DisableTestPing(t *testing.T) {
	_, _, err := startAll()
	assert.NoError(t, err)

	ping := &Ping{
		Table: "customers",
		SourceTargetConfig: SourceTargetConfig{
			Source: vitessContainer.Config(),
			Target: tidbContainer.Config(),
		},
	}
	err = ping.Run()
	assert.NoError(t, err)
}
