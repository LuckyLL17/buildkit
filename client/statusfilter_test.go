package client

import (
	"testing"
	"time"

	controlapi "github.com/moby/buildkit/api/services/control"
	"github.com/moby/buildkit/solver/pb"
	digest "github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

func testStatusFixtures() (digest.Digest, digest.Digest, digest.Digest, time.Time, time.Time, time.Time) {
	v1 := digest.FromString("v1")
	v2 := digest.FromString("v2")
	v3 := digest.FromString("v3")
	t1 := time.Unix(100, 0)
	t2 := t1.Add(time.Second)
	t3 := t2.Add(time.Second)
	return v1, v2, v3, t1, t2, t3
}

func feedStatusStream(p *StatusProcessor, dgst1, dgst2, dgst3 digest.Digest, t1, t2, t3 time.Time) {
	p.Apply(&SolveStatus{Vertexes: []*Vertex{
		{Digest: dgst1, Name: "v1", Started: &t1},
	}})
	p.Apply(&SolveStatus{Vertexes: []*Vertex{
		{Digest: dgst2, Name: "v2", Started: &t2, ProgressGroup: &pb.ProgressGroup{Id: "g1"}},
	}})
	p.Apply(&SolveStatus{Vertexes: []*Vertex{
		{Digest: dgst3, Name: "v3", Started: &t3, ProgressGroup: &pb.ProgressGroup{Id: "g2"}},
	}})
	p.Apply(&SolveStatus{Statuses: []*VertexStatus{
		{ID: "s1", Vertex: dgst1, Name: "pull", Current: 1, Total: 10, Timestamp: t2},
	}})
	p.Apply(&SolveStatus{Statuses: []*VertexStatus{
		{ID: "s1", Vertex: dgst1, Name: "pull", Current: 8, Total: 10, Timestamp: t3},
	}})
	p.Apply(&SolveStatus{Logs: []*VertexLog{
		{Vertex: dgst1, Stream: 2, Data: []byte("hello"), Timestamp: t2},
	}})
	p.Apply(&SolveStatus{Warnings: []*VertexWarning{
		{Vertex: dgst2, Short: []byte("careful")},
	}})
	p.Apply(&SolveStatus{Vertexes: []*Vertex{
		{Digest: dgst1, Name: "v1", Started: &t1, Completed: &t3},
	}})
}

func TestStatusProcessorPassThrough(t *testing.T) {
	d1, d2, d3, t1, t2, t3 := testStatusFixtures()
	p := NewStatusProcessor(StatusOptions{})

	out := p.Apply(&SolveStatus{
		Vertexes: []*Vertex{{Digest: d1, Name: "v1", Started: &t1}, {Digest: d2, Name: "v2", Started: &t2}},
		Statuses: []*VertexStatus{{ID: "s", Vertex: d3, Timestamp: t3}},
		Logs:     []*VertexLog{{Vertex: d1, Data: []byte("x")}},
	})
	require.Len(t, out.Vertexes, 2)
	require.Len(t, out.Statuses, 1)
	require.Len(t, out.Logs, 1)

	// Empty marker messages pass through unchanged.
	marker := p.Apply(&SolveStatus{})
	require.NotNil(t, marker)
	require.False(t, marker.Snapshot)
	require.Nil(t, p.Apply(nil))
}

func TestStatusProcessorVertexDigestFilter(t *testing.T) {
	d1, d2, _, t1, t2, t3 := testStatusFixtures()
	p := NewStatusProcessor(StatusOptions{Filters: []StatusFilter{
		{Vertexes: []string{d1.String()}},
	}})

	out := p.Apply(&SolveStatus{
		Vertexes: []*Vertex{
			{Digest: d1, Name: "v1", Started: &t1},
			{Digest: d2, Name: "v2", Started: &t2},
		},
		Statuses: []*VertexStatus{
			{ID: "s1", Vertex: d1, Timestamp: t2},
			{ID: "s2", Vertex: d2, Timestamp: t3},
		},
		Logs: []*VertexLog{
			{Vertex: d1, Data: []byte("m")},
			{Vertex: d2, Data: []byte("n")},
		},
		Warnings: []*VertexWarning{
			{Vertex: d1, Short: []byte("a")},
			{Vertex: d2, Short: []byte("b")},
		},
	})
	require.Len(t, out.Vertexes, 1)
	require.Equal(t, d1, out.Vertexes[0].Digest)
	require.Len(t, out.Statuses, 1)
	require.Equal(t, d1, out.Statuses[0].Vertex)
	require.Len(t, out.Logs, 1)
	require.Equal(t, d1, out.Logs[0].Vertex)
	require.Len(t, out.Warnings, 1)
	require.Equal(t, d1, out.Warnings[0].Vertex)
}

func TestStatusProcessorVertexNameFilter(t *testing.T) {
	d1, d2, _, t1, t2, _ := testStatusFixtures()
	p := NewStatusProcessor(StatusOptions{Filters: []StatusFilter{
		{Vertexes: []string{"v2"}},
	}})
	out := p.Apply(&SolveStatus{Vertexes: []*Vertex{
		{Digest: d1, Name: "v1", Started: &t1},
		{Digest: d2, Name: "v2", Started: &t2},
	}})
	require.Len(t, out.Vertexes, 1)
	require.Equal(t, d2, out.Vertexes[0].Digest)
}

func TestStatusProcessorProgressGroupFilter(t *testing.T) {
	d1, d2, d3, t1, t2, t3 := testStatusFixtures()
	p := NewStatusProcessor(StatusOptions{Filters: []StatusFilter{
		{ProgressGroup: &pb.ProgressGroup{Id: "g1"}},
	}})
	out := p.Apply(&SolveStatus{Vertexes: []*Vertex{
		{Digest: d1, Name: "v1", Started: &t1},
		{Digest: d2, Name: "v2", Started: &t2, ProgressGroup: &pb.ProgressGroup{Id: "g1"}},
		{Digest: d3, Name: "v3", Started: &t3, ProgressGroup: &pb.ProgressGroup{Id: "g2"}},
	}})
	require.Len(t, out.Vertexes, 1)
	require.Equal(t, d2, out.Vertexes[0].Digest)
}

func TestStatusProcessorTypeFilter(t *testing.T) {
	d1, d2, _, t1, t2, _ := testStatusFixtures()
	p := NewStatusProcessor(StatusOptions{Filters: []StatusFilter{
		{Types: []StatusEventType{StatusEventLog, StatusEventWarning}},
	}})
	// Unknown vertex still passes when the filter only restricts event types.
	unknown := digest.FromString("unknown")
	out := p.Apply(&SolveStatus{
		Vertexes: []*Vertex{{Digest: d1, Name: "v1", Started: &t1}},
		Statuses: []*VertexStatus{{ID: "s", Vertex: d2, Timestamp: t2}},
		Logs:     []*VertexLog{{Vertex: unknown, Data: []byte("m")}},
		Warnings: []*VertexWarning{{Vertex: unknown, Short: []byte("w")}},
	})
	require.Empty(t, out.Vertexes)
	require.Empty(t, out.Statuses)
	require.Len(t, out.Logs, 1)
	require.Len(t, out.Warnings, 1)
}

func TestStatusProcessorCompletedFilter(t *testing.T) {
	d1, d2, _, t1, t2, t3 := testStatusFixtures()
	completed := true
	running := false

	out := NewStatusProcessor(StatusOptions{Filters: []StatusFilter{{Completed: &completed}}}).Apply(&SolveStatus{
		Vertexes: []*Vertex{
			{Digest: d1, Started: &t1, Completed: &t3},
			{Digest: d2, Started: &t2},
		},
	})
	require.Len(t, out.Vertexes, 1)
	require.Equal(t, d1, out.Vertexes[0].Digest)

	out = NewStatusProcessor(StatusOptions{Filters: []StatusFilter{{Completed: &running}}}).Apply(&SolveStatus{
		Vertexes: []*Vertex{
			{Digest: d1, Started: &t1, Completed: &t3},
			{Digest: d2, Started: &t2},
		},
	})
	require.Len(t, out.Vertexes, 1)
	require.Equal(t, d2, out.Vertexes[0].Digest)
}

func TestStatusProcessorMultipleFiltersAreAlternatives(t *testing.T) {
	d1, d2, d3, t1, t2, t3 := testStatusFixtures()
	p := NewStatusProcessor(StatusOptions{Filters: []StatusFilter{
		{Vertexes: []string{"v1"}},
		{ProgressGroup: &pb.ProgressGroup{Id: "g2"}},
	}})
	out := p.Apply(&SolveStatus{Vertexes: []*Vertex{
		{Digest: d1, Name: "v1", Started: &t1},
		{Digest: d2, Name: "v2", Started: &t2, ProgressGroup: &pb.ProgressGroup{Id: "g1"}},
		{Digest: d3, Name: "v3", Started: &t3, ProgressGroup: &pb.ProgressGroup{Id: "g2"}},
	}})
	require.Len(t, out.Vertexes, 2)
	require.Equal(t, d1, out.Vertexes[0].Digest)
	require.Equal(t, d3, out.Vertexes[1].Digest)
}

func TestStatusProcessorSnapshot(t *testing.T) {
	d1, d2, d3, t1, t2, t3 := testStatusFixtures()
	p := NewStatusProcessor(StatusOptions{Filters: []StatusFilter{{}}})
	feedStatusStream(p, d1, d2, d3, t1, t2, t3)

	snap := p.Snapshot()
	require.True(t, snap.Snapshot)
	require.Len(t, snap.Vertexes, 3)
	// Stable ordering: by start time.
	require.Equal(t, []digest.Digest{d1, d2, d3}, []digest.Digest{
		snap.Vertexes[0].Digest, snap.Vertexes[1].Digest, snap.Vertexes[2].Digest,
	})
	// Latest value per status ID wins.
	require.Len(t, snap.Statuses, 1)
	require.Equal(t, int64(8), snap.Statuses[0].Current)
	// Warnings are part of the state, logs are pure increments.
	require.Len(t, snap.Warnings, 1)
	require.Empty(t, snap.Logs)
	// Latest vertex state wins.
	require.Equal(t, &t3, snap.Vertexes[0].Completed)
}

func TestStatusProcessorSnapshotFiltered(t *testing.T) {
	d1, d2, d3, t1, t2, t3 := testStatusFixtures()
	completed := true
	p := NewStatusProcessor(StatusOptions{Filters: []StatusFilter{
		{Types: []StatusEventType{StatusEventVertex}, Completed: &completed},
	}})
	feedStatusStream(p, d1, d2, d3, t1, t2, t3)

	snap := p.Snapshot()
	require.True(t, snap.Snapshot)
	require.Len(t, snap.Vertexes, 1)
	require.Equal(t, d1, snap.Vertexes[0].Digest)
	require.Empty(t, snap.Statuses)
	require.Empty(t, snap.Warnings)
}

func TestSnapshotMessagesChunking(t *testing.T) {
	ss := &SolveStatus{Snapshot: true}
	for i := 0; i < snapshotChunkLimit+1; i++ {
		ss.Vertexes = append(ss.Vertexes, &Vertex{Digest: digest.FromString(string(rune('a'+i%26)) + string(rune(i)))})
	}
	for i := 0; i < snapshotChunkLimit+2; i++ {
		ss.Statuses = append(ss.Statuses, &VertexStatus{ID: string(rune(i)), Timestamp: time.Unix(int64(i), 0)})
	}
	msgs := SnapshotMessages(ss)
	require.Len(t, msgs, 4)
	for _, m := range msgs {
		require.True(t, m.Snapshot)
	}
	require.Len(t, msgs[0].Vertexes, snapshotChunkLimit)
	require.Len(t, msgs[1].Vertexes, 1)
	require.Len(t, msgs[2].Statuses, snapshotChunkLimit)
	require.Len(t, msgs[3].Statuses, 2)
	require.Nil(t, SnapshotMessages(&SolveStatus{Snapshot: true}))
}

func TestStatusOptionsPBMapping(t *testing.T) {
	completed := true
	opts := StatusOptions{
		Snapshot: true,
		Filters: []StatusFilter{{
			Vertexes:      []string{"sha256:deadbeef", "name"},
			ProgressGroup: &pb.ProgressGroup{Id: "g1"},
			Types:         []StatusEventType{StatusEventVertex, StatusEventLog},
			Completed:     &completed,
		}},
	}
	pbFilters := opts.ToPBFilters()
	require.Len(t, pbFilters, 1)
	require.Equal(t, []string{"sha256:deadbeef", "name"}, pbFilters[0].Vertexes)
	require.Equal(t, "g1", pbFilters[0].ProgressGroup.Id)
	require.Equal(t, []controlapi.StatusEventType{
		controlapi.StatusEventType_VERTEX,
		controlapi.StatusEventType_LOG,
	}, pbFilters[0].Types)
	require.Equal(t, controlapi.StatusCompletion_COMPLETION_COMPLETED, pbFilters[0].Completed)

	roundtrip := StatusOptionsFromPB(&controlapi.StatusRequest{
		Ref:      "r1",
		Filters:  pbFilters,
		Snapshot: true,
	})
	require.True(t, roundtrip.Snapshot)
	require.Len(t, roundtrip.Filters, 1)
	require.Equal(t, opts.Filters[0].Vertexes, roundtrip.Filters[0].Vertexes)
	require.Equal(t, opts.Filters[0].ProgressGroup.Id, roundtrip.Filters[0].ProgressGroup.Id)
	require.Equal(t, opts.Filters[0].Types, roundtrip.Filters[0].Types)
	require.NotNil(t, roundtrip.Filters[0].Completed)
	require.True(t, *roundtrip.Filters[0].Completed)

	// Default completion maps both ways to nil/ANY.
	require.Nil(t, StatusFiltersFromPB([]*controlapi.StatusFilter{{}})[0].Completed)
	require.Equal(t, controlapi.StatusCompletion_COMPLETION_ANY,
		StatusFiltersToPB([]StatusFilter{{}})[0].Completed)
}
