package worker

import (
	"context"
	"testing"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/client/llb/sourceresolver"
	"github.com/moby/buildkit/executor"
	"github.com/moby/buildkit/exporter"
	"github.com/moby/buildkit/frontend"
	"github.com/moby/buildkit/session"
	containerdsnapshot "github.com/moby/buildkit/snapshot/containerd"
	"github.com/moby/buildkit/solver"
	"github.com/moby/buildkit/solver/llbsolver/cdidevices"
	"github.com/moby/buildkit/solver/pb"
	"github.com/moby/buildkit/source"
	"github.com/moby/buildkit/util/leaseutil"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/stretchr/testify/require"
)

type stubWorker struct {
	id        string
	labels    map[string]string
	platforms []ocispecs.Platform
	version   client.BuildkitVersion
}

func (w *stubWorker) Close() error { return nil }
func (w *stubWorker) ID() string   { return w.id }
func (w *stubWorker) Labels() map[string]string {
	return w.labels
}
func (w *stubWorker) Platforms(bool) []ocispecs.Platform {
	return w.platforms
}
func (w *stubWorker) BuildkitVersion() client.BuildkitVersion {
	return w.version
}
func (w *stubWorker) GCPolicy() []client.PruneInfo { return nil }
func (w *stubWorker) LoadRef(context.Context, string, bool) (cache.ImmutableRef, error) {
	return nil, nil
}
func (w *stubWorker) ResolveOp(solver.Vertex, frontend.FrontendLLBBridge, *session.Manager, ProxyOpt) (solver.Op, error) {
	return nil, nil
}
func (w *stubWorker) ParseSource(*pb.SourceOp, *pb.Platform) (source.Identifier, error) {
	return nil, nil
}
func (w *stubWorker) ResolveSourceMetadata(context.Context, *pb.SourceOp, sourceresolver.Opt, *session.Manager, solver.JobContext) (*sourceresolver.MetaResponse, error) {
	return nil, nil
}
func (w *stubWorker) DiskUsage(context.Context, client.DiskUsageInfo) ([]*client.UsageInfo, error) {
	return nil, nil
}
func (w *stubWorker) Exporter(string, *session.Manager) (exporter.Exporter, error) {
	return nil, nil
}
func (w *stubWorker) Prune(context.Context, chan client.UsageInfo, ...client.PruneInfo) error {
	return nil
}
func (w *stubWorker) FromRemote(context.Context, *solver.Remote) (cache.ImmutableRef, error) {
	return nil, nil
}
func (w *stubWorker) PruneCacheMounts(context.Context, map[string]bool) error { return nil }
func (w *stubWorker) ContentStore() *containerdsnapshot.Store                 { return nil }
func (w *stubWorker) Executor() executor.Executor                             { return nil }
func (w *stubWorker) CacheManager() cache.Manager                             { return nil }
func (w *stubWorker) LeaseManager() *leaseutil.Manager                        { return nil }
func (w *stubWorker) GarbageCollect(context.Context) error                    { return nil }
func (w *stubWorker) CDIManager() *cdidevices.Manager                         { return nil }

var _ Worker = &stubWorker{}

func platform(osName, arch string) ocispecs.Platform {
	return ocispecs.Platform{OS: osName, Architecture: arch}
}

func testWorkers() []Worker {
	return []Worker{
		&stubWorker{
			id:        "worker-arm",
			labels:    map[string]string{"org.mobyproject.buildkit.worker.snapshotter": "native", "zone": "edge"},
			platforms: []ocispecs.Platform{platform("linux", "arm64")},
			version:   client.BuildkitVersion{Version: "v0.12.0"},
		},
		&stubWorker{
			id:        "worker-amd",
			labels:    map[string]string{"org.mobyproject.buildkit.worker.snapshotter": "overlay"},
			platforms: []ocispecs.Platform{platform("linux", "amd64"), platform("linux", "arm64")},
			version:   client.BuildkitVersion{Version: "v0.13.1+unknown"},
		},
	}
}

func TestSelectByPlatform(t *testing.T) {
	cands, err := MatchProfile(context.Background(), testWorkers(), client.CapabilityProfile{
		Platforms: []ocispecs.Platform{platform("linux", "amd64")},
	})
	require.NoError(t, err)
	require.Len(t, cands, 2)

	require.True(t, cands[0].Match.Compatible)
	require.Equal(t, "worker-amd", cands[0].Worker.ID())

	require.False(t, cands[1].Match.Compatible)
	require.Equal(t, "worker-arm", cands[1].Worker.ID())
	require.Contains(t, cands[1].Match.Missing, "platform linux/amd64 is not supported")
}

func TestSelectMultiPlatformRequiresAll(t *testing.T) {
	workers := testWorkers()
	cands, err := MatchProfile(context.Background(), workers, client.CapabilityProfile{
		Platforms: []ocispecs.Platform{platform("linux", "amd64"), platform("linux", "arm64")},
	})
	require.NoError(t, err)

	require.True(t, cands[0].Match.Compatible)
	require.Equal(t, "worker-amd", cands[0].Worker.ID())
	require.False(t, cands[1].Match.Compatible)
}

func TestSelectBySnapshotterLabel(t *testing.T) {
	t.Run("exact match preferred", func(t *testing.T) {
		cands, err := MatchProfile(context.Background(), testWorkers(), client.CapabilityProfile{
			Labels: map[string]string{"org.mobyproject.buildkit.worker.snapshotter": "overlay"},
		})
		require.NoError(t, err)
		require.Equal(t, "worker-amd", cands[0].Worker.ID())
		require.True(t, cands[0].Match.Compatible)
		require.False(t, cands[1].Match.Compatible)
		require.Contains(t, cands[1].Match.Conflicts, `label "org.mobyproject.buildkit.worker.snapshotter" is "native", expected "overlay"`)
	})
	t.Run("presence only", func(t *testing.T) {
		cands, err := MatchProfile(context.Background(), testWorkers(), client.CapabilityProfile{
			Labels: map[string]string{"zone": ""},
		})
		require.NoError(t, err)
		require.Equal(t, "worker-arm", cands[0].Worker.ID())
		require.True(t, cands[0].Match.Compatible)
		require.Contains(t, cands[1].Match.Missing, `label "zone" is not set`)
	})
}

func TestSelectVersionConstraint(t *testing.T) {
	cands, err := MatchProfile(context.Background(), testWorkers(), client.CapabilityProfile{
		VersionConstraint: ">=0.13.0,<1.0.0",
	})
	require.NoError(t, err)
	require.Equal(t, "worker-amd", cands[0].Worker.ID())
	require.True(t, cands[0].Match.Compatible)
	require.False(t, cands[1].Match.Compatible)
	require.Contains(t, cands[1].Match.Conflicts, `buildkit version "v0.12.0" does not satisfy ">=0.13.0,<1.0.0"`)
}

func TestSelectInvalidVersionConstraint(t *testing.T) {
	_, err := MatchProfile(context.Background(), testWorkers(), client.CapabilityProfile{
		VersionConstraint: "~0.13.0",
	})
	require.Error(t, err)

	_, err = MatchProfile(context.Background(), testWorkers(), client.CapabilityProfile{
		VersionConstraint: ">=0.13.0,",
	})
	require.Error(t, err)
}

func TestSelectOrderIsStable(t *testing.T) {
	workers := []Worker{
		&stubWorker{id: "zeta", platforms: []ocispecs.Platform{platform("linux", "amd64")}},
		&stubWorker{id: "alpha", platforms: []ocispecs.Platform{platform("linux", "amd64")}},
		&stubWorker{id: "mid", platforms: []ocispecs.Platform{platform("linux", "amd64")}},
	}
	for range 10 {
		cands, err := MatchProfile(context.Background(), workers, client.CapabilityProfile{
			Platforms: []ocispecs.Platform{platform("linux", "amd64")},
		})
		require.NoError(t, err)
		require.Equal(t, []string{"alpha", "mid", "zeta"}, []string{
			cands[0].Worker.ID(), cands[1].Worker.ID(), cands[2].Worker.ID(),
		})
	}
}

func TestSelectSpecializedWorkerRanksHigher(t *testing.T) {
	workers := []Worker{
		&stubWorker{id: "generic", platforms: []ocispecs.Platform{platform("linux", "amd64"), platform("linux", "arm64")}},
		&stubWorker{id: "specialized", platforms: []ocispecs.Platform{platform("linux", "amd64")}},
	}
	cands, err := MatchProfile(context.Background(), workers, client.CapabilityProfile{
		Platforms: []ocispecs.Platform{platform("linux", "amd64")},
	})
	require.NoError(t, err)
	require.Equal(t, "specialized", cands[0].Worker.ID())
	require.Greater(t, cands[0].Match.Rank, cands[1].Match.Rank)
}

func TestSelectEmptyProfile(t *testing.T) {
	cands, err := MatchProfile(context.Background(), testWorkers(), client.CapabilityProfile{})
	require.NoError(t, err)
	require.Len(t, cands, 2)
	for _, cand := range cands {
		require.True(t, cand.Match.Compatible)
		require.Nil(t, cand.Match.Missing)
		require.Nil(t, cand.Match.Conflicts)
	}
}

func TestControllerSelect(t *testing.T) {
	var c Controller
	for _, w := range testWorkers() {
		require.NoError(t, c.Add(w))
	}
	cands, err := c.Select(context.Background(), client.CapabilityProfile{
		Platforms: []ocispecs.Platform{platform("linux", "arm64")},
	})
	require.NoError(t, err)
	require.Len(t, cands, 2)
	require.True(t, cands[0].Match.Compatible)
	require.True(t, cands[1].Match.Compatible)
}

func TestMatchCDIDevice(t *testing.T) {
	devices := []cdidevices.Device{
		{Name: "vendor.com/gpu=0"},
		{Name: "vendor.com/gpu=1"},
		{Name: "vendor.com/fpga=0"},
		{Name: "vendor.com/ondemand", OnDemand: true},
	}

	rank, ok := matchCDIDevice("vendor.com/gpu=1", devices)
	require.True(t, ok)
	require.Equal(t, 2, rank)

	rank, ok = matchCDIDevice("vendor.com/gpu", devices)
	require.True(t, ok)
	require.Equal(t, 1, rank)

	_, ok = matchCDIDevice("vendor.com/gpu=9", devices)
	require.False(t, ok)

	rank, ok = matchCDIDevice("vendor.com/ondemand", devices)
	require.True(t, ok)
	require.Equal(t, 2, rank)

	_, ok = matchCDIDevice("vendor.com/missing", devices)
	require.False(t, ok)
}

func TestVersionComparison(t *testing.T) {
	tests := []struct {
		constraint string
		version    string
		match      bool
	}{
		{">=0.13.0", "v0.13.1+unknown", true},
		{">=0.13.0", "0.13.0", true},
		{">0.13.0", "0.13.0", false},
		{"<1.0.0", "0.99.0", true},
		{"<=1.0.0", "1.0.0", true},
		{"!=0.12.0", "0.13.0", true},
		{"=0.13.0", "0.13.0", true},
		{"0.13.0", "0.13.0", true},
		{">=0.13", "0.13.1", true},
	}
	for _, tt := range tests {
		conds, err := parseVersionConstraints(tt.constraint)
		require.NoError(t, err, tt.constraint)
		v, err := parseVersion(tt.version)
		require.NoError(t, err, tt.version)
		allMatch := true
		for _, cnd := range conds {
			if !cnd.matches(v) {
				allMatch = false
			}
		}
		require.Equal(t, tt.match, allMatch, "%s vs %s", tt.version, tt.constraint)
	}
}
