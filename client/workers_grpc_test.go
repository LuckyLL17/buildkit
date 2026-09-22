package client

import (
	"context"
	"net"
	"testing"

	controlapi "github.com/moby/buildkit/api/services/control"
	apitypes "github.com/moby/buildkit/api/types"
	"github.com/moby/buildkit/solver/pb"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type fakeWorkersServer struct {
	controlapi.UnimplementedControlServer
	requests []*controlapi.ListWorkersRequest
}

func (s *fakeWorkersServer) ListWorkers(_ context.Context, req *controlapi.ListWorkersRequest) (*controlapi.ListWorkersResponse, error) {
	s.requests = append(s.requests, req)
	var capability0, capability1 *apitypes.WorkerCapability
	if req.Profile != nil {
		capability0 = &apitypes.WorkerCapability{Compatible: true, Rank: 4}
		capability1 = &apitypes.WorkerCapability{
			Compatible: false,
			Missing:    []string{"platform linux/amd64 is not supported"},
			Conflicts:  []string{`buildkit version "0.12.0" does not satisfy ">=0.13.0"`},
		}
	}
	records := []*apitypes.WorkerRecord{
		{
			ID:        "worker0",
			Platforms: []*pb.Platform{{OS: "linux", Architecture: "amd64"}},
			Labels:    map[string]string{"org.mobyproject.buildkit.worker.snapshotter": "overlay"},
			BuildkitVersion: &apitypes.BuildkitVersion{
				Package: "github.com/moby/buildkit",
				Version: "0.13.1",
			},
			Capability: capability0,
		},
		{
			ID:        "worker1",
			Platforms: []*pb.Platform{{OS: "linux", Architecture: "arm64"}},
			BuildkitVersion: &apitypes.BuildkitVersion{
				Version: "0.12.0",
			},
			Capability: capability1,
		},
	}
	return &controlapi.ListWorkersResponse{Record: records}, nil
}

func newWorkersTestClient(t *testing.T) (*Client, *fakeWorkersServer) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	srv := grpc.NewServer()
	fake := &fakeWorkersServer{}
	controlapi.RegisterControlServer(srv, fake)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)

	c, err := New(context.Background(), "localhost", WithContextDialer(func(context.Context, string) (net.Conn, error) {
		return net.Dial("tcp", lis.Addr().String())
	}))
	require.NoError(t, err)
	t.Cleanup(func() { c.Close() })
	return c, fake
}

func TestListWorkersWithCapabilityProfileRoundtrip(t *testing.T) {
	c, srv := newWorkersTestClient(t)

	profile := CapabilityProfile{
		Platforms:         []ocispecs.Platform{{OS: "linux", Architecture: "amd64"}},
		Labels:            map[string]string{"org.mobyproject.buildkit.worker.snapshotter": "overlay"},
		CDIDevices:        []string{"vendor.com/gpu=0"},
		VersionConstraint: ">=0.13.0",
	}
	workers, err := c.ListWorkers(context.Background(), WithCapabilityProfile(profile))
	require.NoError(t, err)
	require.Len(t, srv.requests, 1)
	require.NotNil(t, srv.requests[0].Profile)
	require.Equal(t, "linux", srv.requests[0].Profile.Platforms[0].OS)
	require.Equal(t, "amd64", srv.requests[0].Profile.Platforms[0].Architecture)
	require.Equal(t, profile.Labels, srv.requests[0].Profile.Labels)
	require.Equal(t, profile.CDIDevices, srv.requests[0].Profile.CDIDevices)
	require.Equal(t, profile.VersionConstraint, srv.requests[0].Profile.VersionConstraint)

	require.Len(t, workers, 2)
	require.True(t, workers[0].Capability.Compatible)
	require.Equal(t, 4, workers[0].Capability.Rank)
	require.False(t, workers[1].Capability.Compatible)
	require.Contains(t, workers[1].Capability.Missing, "platform linux/amd64 is not supported")
	require.Contains(t, workers[1].Capability.Conflicts, `buildkit version "0.12.0" does not satisfy ">=0.13.0"`)
}

func TestListWorkersWithoutProfileKeepsShape(t *testing.T) {
	c, srv := newWorkersTestClient(t)

	workers, err := c.ListWorkers(context.Background(), WithFilter([]string{"id=worker0"}))
	require.NoError(t, err)
	require.Len(t, srv.requests, 1)
	require.Nil(t, srv.requests[0].Profile)
	require.Equal(t, []string{"id=worker0"}, srv.requests[0].Filter)

	require.Len(t, workers, 2)
	require.Equal(t, "worker0", workers[0].ID)
	require.Nil(t, workers[0].Capability)
	require.Nil(t, workers[1].Capability)
}

func TestSelectWorkersConcurrent(t *testing.T) {
	c, srv := newWorkersTestClient(t)

	profiles := []CapabilityProfile{
		{Platforms: []ocispecs.Platform{{OS: "linux", Architecture: "amd64"}}},
		{VersionConstraint: ">=0.13.0"},
		{Labels: map[string]string{"zone": "edge"}},
	}
	results, err := c.SelectWorkers(context.Background(), profiles, WithFilter([]string{"id=worker0"}))
	require.NoError(t, err)
	require.Len(t, results, 3)
	for i, res := range results {
		require.Equal(t, profiles[i], res.Profile)
		require.Len(t, res.Workers, 2)
		for _, w := range res.Workers {
			require.NotNil(t, w.Capability)
		}
	}
	require.Len(t, srv.requests, 3)
	for _, req := range srv.requests {
		require.Equal(t, []string{"id=worker0"}, req.Filter)
		require.NotNil(t, req.Profile)
	}
}
