package client

import (
	"testing"

	apitypes "github.com/moby/buildkit/api/types"
	"github.com/moby/buildkit/solver/pb"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

func TestListWorkersFilterOption(t *testing.T) {
	info := &ListWorkersInfo{}
	WithFilter([]string{"id=worker0"}).SetListWorkersOption(info)
	require.Equal(t, []string{"id=worker0"}, info.Filter)
	require.Nil(t, info.Profile)
}

func TestCapabilityProfileOption(t *testing.T) {
	profile := CapabilityProfile{
		Platforms:         []ocispecs.Platform{{OS: "linux", Architecture: "arm64"}},
		Labels:            map[string]string{"org.mobyproject.buildkit.worker.snapshotter": "overlay"},
		CDIDevices:        []string{"vendor.com/gpu=0"},
		VersionConstraint: ">=0.13.0",
	}
	info := &ListWorkersInfo{}
	WithCapabilityProfile(profile).SetListWorkersOption(info)
	require.NotNil(t, info.Profile)
	require.Equal(t, profile, *info.Profile)
}

func TestCapabilityProfileEmpty(t *testing.T) {
	require.True(t, CapabilityProfile{}.Empty())
	require.False(t, (CapabilityProfile{Platforms: []ocispecs.Platform{{OS: "linux"}}}).Empty())
	require.False(t, (CapabilityProfile{Labels: map[string]string{"a": "b"}}).Empty())
	require.False(t, (CapabilityProfile{CDIDevices: []string{"x"}}).Empty())
	require.False(t, (CapabilityProfile{VersionConstraint: ">=1.0.0"}).Empty())
}

func TestCapabilityProfileAPIRoundtrip(t *testing.T) {
	profile := CapabilityProfile{
		Platforms:         []ocispecs.Platform{{OS: "linux", Architecture: "arm64", Variant: "v8"}},
		Labels:            map[string]string{"zone": "edge"},
		CDIDevices:        []string{"vendor.com/gpu=0", "vendor.com/fpga=1"},
		VersionConstraint: ">=0.13.0,<1.0.0",
	}
	apiProfile := toAPICapabilityProfile(&profile)
	require.Equal(t, []string{"vendor.com/gpu=0", "vendor.com/fpga=1"}, apiProfile.CDIDevices)
	require.Equal(t, ">=0.13.0,<1.0.0", apiProfile.VersionConstraint)
	require.Equal(t, map[string]string{"zone": "edge"}, apiProfile.Labels)
	require.Len(t, apiProfile.Platforms, 1)

	back := CapabilityProfile{
		Platforms:         pb.ToSpecPlatforms(apiProfile.Platforms),
		Labels:            apiProfile.Labels,
		CDIDevices:        apiProfile.CDIDevices,
		VersionConstraint: apiProfile.VersionConstraint,
	}
	require.Equal(t, profile.Platforms, back.Platforms)
	require.Equal(t, profile.Labels, back.Labels)
	require.Equal(t, profile.CDIDevices, back.CDIDevices)
	require.Equal(t, profile.VersionConstraint, back.VersionConstraint)
}

func TestCapabilityProfileAPINil(t *testing.T) {
	require.Nil(t, toAPICapabilityProfile(nil))
	require.Nil(t, fromAPIWorkerCapability(nil))
}

func TestWorkerCapabilityConversion(t *testing.T) {
	in := &apitypes.WorkerCapability{
		Compatible: false,
		Rank:       3,
		Missing:    []string{"platform linux/arm64 is not supported"},
		Conflicts:  []string{`buildkit version "v0.12.0" does not satisfy ">=0.13.0"`},
	}
	out := fromAPIWorkerCapability(in)
	require.False(t, out.Compatible)
	require.Equal(t, 3, out.Rank)
	require.Equal(t, in.Missing, out.Missing)
	require.Equal(t, in.Conflicts, out.Conflicts)
}
