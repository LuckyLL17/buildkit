package main

import (
	"testing"

	"github.com/moby/buildkit/util/testutil/integration"
	"github.com/stretchr/testify/require"
)

func testPrune(t *testing.T, sb integration.Sandbox) {
	cmd := sb.Cmd("prune")
	err := cmd.Run()
	require.NoError(t, err)
}

func testPrunePlan(t *testing.T, sb integration.Sandbox) {
	cmd := sb.Cmd("prune", "--plan")
	dt, err := cmd.CombinedOutput()
	require.NoError(t, err, string(dt))
	require.Contains(t, string(dt), "Total reclaimable:")
}
