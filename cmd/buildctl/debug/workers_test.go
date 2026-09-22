package debug

import (
	"bytes"
	"strings"
	"testing"
	"text/tabwriter"

	"github.com/containerd/platforms"
	"github.com/moby/buildkit/client"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestParseLabelRequirements(t *testing.T) {
	labels, err := parseLabelRequirements([]string{"zone=edge", "present="})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"zone": "edge", "present": ""}, labels)

	presence, err := parseLabelRequirements([]string{"present"})
	require.NoError(t, err)
	require.Equal(t, map[string]string{"present": ""}, presence)

	_, err = parseLabelRequirements([]string{"=value"})
	require.Error(t, err)
}

func TestNewCapabilityProfile(t *testing.T) {
	profile, err := newCapabilityProfile(
		[]string{"linux/arm64"},
		map[string]string{"zone": "edge"},
		[]string{"vendor.com/gpu=0"},
		">=0.13.0",
	)
	require.NoError(t, err)
	require.Equal(t, platforms.Normalize(platforms.MustParse("linux/arm64")), profile.Platforms[0])
	require.Equal(t, "edge", profile.Labels["zone"])
	require.Equal(t, []string{"vendor.com/gpu=0"}, profile.CDIDevices)
	require.Equal(t, ">=0.13.0", profile.VersionConstraint)

	_, err = newCapabilityProfile([]string{"not-a-platform"}, nil, nil, "")
	require.Error(t, err)
}

func TestFormatCapabilityProfile(t *testing.T) {
	profile, err := newCapabilityProfile(
		[]string{"linux/arm64", "linux/amd64"},
		map[string]string{"zone": "edge"},
		[]string{"vendor.com/gpu=0"},
		">=0.13.0",
	)
	require.NoError(t, err)
	formatted := formatCapabilityProfile(profile)
	require.Contains(t, formatted, "platforms=linux/arm64,linux/amd64")
	require.Contains(t, formatted, "labels=zone=edge")
	require.Contains(t, formatted, "cdi-devices=vendor.com/gpu=0")
	require.Contains(t, formatted, "version=>=0.13.0")

	require.Empty(t, formatCapabilityProfile(client.CapabilityProfile{}))
}

func TestPrintSelectionTable(t *testing.T) {
	workers := []*client.WorkerInfo{
		{
			ID:        "good",
			Platforms: []ocispecs.Platform{{OS: "linux", Architecture: "amd64"}},
			Capability: &client.CapabilityMatch{
				Compatible: true,
				Rank:       4,
			},
		},
		{
			ID:        "bad",
			Platforms: []ocispecs.Platform{{OS: "linux", Architecture: "arm64"}},
			Capability: &client.CapabilityMatch{
				Compatible: false,
				Rank:       0,
				Missing:    []string{"platform linux/amd64 is not supported"},
				Conflicts:  []string{`buildkit version "0.12.0" does not satisfy ">=0.13.0"`},
			},
		},
	}

	var b bytes.Buffer
	tw := tabwriter.NewWriter(&b, 1, 8, 1, ' ', 0)
	printSelectionTable(tw, workers)

	out := b.String()
	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Contains(t, lines[0], "ID")
	require.Contains(t, lines[0], "COMPATIBLE")
	require.Contains(t, lines[0], "RANK")
	require.Contains(t, out, "good")
	require.Contains(t, out, "true")
	require.Contains(t, out, "Rejected:")
	require.Contains(t, out, "missing platform linux/amd64 is not supported")
	require.Contains(t, out, `conflict buildkit version "0.12.0" does not satisfy ">=0.13.0"`)
}

func TestPrintWorkersTableWithoutCapability(t *testing.T) {
	workers := []*client.WorkerInfo{
		{ID: "plain", Platforms: []ocispecs.Platform{{OS: "linux", Architecture: "amd64"}}},
	}
	var b bytes.Buffer
	tw := tabwriter.NewWriter(&b, 1, 8, 1, ' ', 0)
	printWorkersTable(tw, workers)

	out := b.String()
	require.Contains(t, out, "ID")
	require.Contains(t, out, "PLATFORMS")
	require.Contains(t, out, "plain")
	require.NotContains(t, out, "COMPATIBLE")
}

