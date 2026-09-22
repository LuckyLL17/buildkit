package solver

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/solver/pb"
	"github.com/moby/buildkit/util/progress"
	digest "github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
)

type statusTestEnv struct {
	j       *Job
	ctx     context.Context
	pw      progress.Writer
	closer  func(error)
	mu      sync.Mutex
	barrier map[string]chan struct{}
}

func newStatusTestEnv(t *testing.T) *statusTestEnv {
	t.Helper()
	pr, pctx, closer := progress.NewContext(t.Context())
	pw, _, _ := progress.NewFromContext(pctx)
	j := &Job{pr: progress.NewMultiReader(pr)}

	env := &statusTestEnv{
		j:       j,
		ctx:     pctx,
		pw:      pw,
		closer:  closer,
		barrier: map[string]chan struct{}{},
	}

	prime := j.pr.Reader(t.Context())
	go func() {
		for {
			p, err := prime.Read(t.Context())
			if err != nil {
				return
			}
			env.mu.Lock()
			for _, e := range p {
				if ch, ok := env.barrier[e.ID]; ok {
					select {
					case <-ch:
					default:
						close(ch)
					}
				}
			}
			env.mu.Unlock()
		}
	}()
	return env
}

func (e *statusTestEnv) write(id string, v any) {
	_ = e.pw.Write(id, v)
}

// flush forces all previous writes to be delivered to the MultiReader by
// sending a uniquely identified barrier event and waiting for its delivery.
func (e *statusTestEnv) flush(t *testing.T, name string) {
	t.Helper()
	id := "barrier-" + name
	e.mu.Lock()
	ch := make(chan struct{})
	e.barrier[id] = ch
	e.mu.Unlock()
	e.write(id, "barrier")
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timeout waiting for barrier %s", name)
	}
}

func (e *statusTestEnv) vertexWriter(dgst digest.Digest) progress.Writer {
	pw, _, _ := progress.NewFromContext(e.ctx, progress.WithMetadata("vertex", dgst))
	return pw
}

func (e *statusTestEnv) close() {
	e.closer(context.Canceled)
	_ = e.pw.Close()
}

type collectedStatus struct {
	mu     sync.Mutex
	all    []*client.SolveStatus
	closed chan struct{}
}

func collectStatus(ch <-chan *client.SolveStatus) *collectedStatus {
	c := &collectedStatus{closed: make(chan struct{})}
	go func() {
		for ss := range ch {
			c.mu.Lock()
			c.all = append(c.all, ss)
			c.mu.Unlock()
		}
		close(c.closed)
	}()
	return c
}

func (c *collectedStatus) snapshot(t *testing.T) *client.SolveStatus {
	t.Helper()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, ss := range c.all {
			if ss.Snapshot {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond)
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, ss := range c.all {
			if !ss.Snapshot {
				return true
			}
		}
		return false
	}, 5*time.Second, time.Millisecond)
	merged := &client.SolveStatus{Snapshot: true}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ss := range c.all {
		if !ss.Snapshot {
			continue
		}
		merged.Vertexes = append(merged.Vertexes, ss.Vertexes...)
		merged.Statuses = append(merged.Statuses, ss.Statuses...)
		merged.Logs = append(merged.Logs, ss.Logs...)
		merged.Warnings = append(merged.Warnings, ss.Warnings...)
	}
	return merged
}

func (c *collectedStatus) waitVertex(t *testing.T, dgst digest.Digest) *client.Vertex {
	t.Helper()
	require.Eventually(t, func() bool {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, ss := range c.all {
			if ss.Snapshot {
				continue
			}
			for _, v := range ss.Vertexes {
				if v.Digest == dgst {
					return true
				}
			}
		}
		return false
	}, 5*time.Second, time.Millisecond)
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ss := range c.all {
		if ss.Snapshot {
			continue
		}
		for _, v := range ss.Vertexes {
			if v.Digest == dgst {
				return v
			}
		}
	}
	return nil
}

func (c *collectedStatus) terminalVertexes() map[digest.Digest]*client.Vertex {
	out := map[digest.Digest]*client.Vertex{}
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ss := range c.all {
		if ss.Snapshot {
			continue
		}
		for _, v := range ss.Vertexes {
			out[v.Digest] = v
		}
	}
	return out
}

func TestJobStatusSnapshot(t *testing.T) {
	env := newStatusTestEnv(t)

	d1 := digest.FromString("v1")
	d2 := digest.FromString("v2")
	start1 := time.Unix(100, 0)
	start2 := start1.Add(time.Second)
	complete := start2.Add(time.Second)

	env.write("v1-queued", client.Vertex{Digest: d1, Name: "v1"})
	env.flush(t, "v1-queued")
	env.write("v1-started", client.Vertex{Digest: d1, Name: "v1", Started: &start1})
	env.flush(t, "v1-started")

	sub := env.vertexWriter(d1)
	require.NoError(t, sub.Write("s1", progress.Status{Action: "pull", Current: 1, Total: 10}))
	env.flush(t, "s1-1")
	require.NoError(t, sub.Write("s1", progress.Status{Action: "pull", Current: 8, Total: 10}))
	env.flush(t, "s1-8")
	require.NoError(t, sub.Write("l1", client.VertexLog{Stream: 2, Data: []byte("hello")}))
	env.flush(t, "l1")
	_ = sub.Close()

	env.write("v1-completed", client.Vertex{Digest: d1, Name: "v1", Started: &start1, Completed: &complete})
	env.flush(t, "v1-completed")
	env.write("v2-started", client.Vertex{Digest: d2, Name: "v2", Started: &start2})
	env.flush(t, "v2-started")

	ch := make(chan *client.SolveStatus, 64)
	go func() {
		_ = env.j.Status(t.Context(), ch, client.StatusOptions{Snapshot: true})
	}()
	coll := collectStatus(ch)

	snapshot := coll.snapshot(t)
	require.Len(t, snapshot.Vertexes, 2)
	require.Equal(t, d1, snapshot.Vertexes[0].Digest)
	require.NotNil(t, snapshot.Vertexes[0].Completed)
	require.Equal(t, d2, snapshot.Vertexes[1].Digest)
	require.Len(t, snapshot.Statuses, 1)
	require.Equal(t, int64(8), snapshot.Statuses[0].Current)
	// Logs are stream events, never part of the snapshot.
	require.Empty(t, snapshot.Logs)

	// Incremental events after the snapshot.
	d3 := digest.FromString("v3")
	v3Start := time.Now()
	env.write("v3-started", client.Vertex{Digest: d3, Name: "v3", Started: &v3Start})
	v3 := coll.waitVertex(t, d3)
	require.NotNil(t, v3)

	// Terminal convergence: in-flight vertexes get a synthesized completion.
	env.close()
	<-coll.closed

	terminal := coll.terminalVertexes()
	require.Contains(t, terminal, d2)
	require.Contains(t, terminal, d3)
	for _, dgst := range []digest.Digest{d2, d3} {
		require.NotNil(t, terminal[dgst].Completed)
		require.Equal(t, context.Canceled.Error(), terminal[dgst].Error)
	}
}

func TestJobStatusSnapshotFilter(t *testing.T) {
	env := newStatusTestEnv(t)

	d1 := digest.FromString("v1")
	d2 := digest.FromString("v2")
	t1 := time.Unix(100, 0)
	t2 := t1.Add(time.Second)
	env.write("v1", client.Vertex{Digest: d1, Name: "v1", Started: &t1})
	env.flush(t, "v1")
	env.write("v2", client.Vertex{
		Digest:        d2,
		Name:          "v2",
		Started:       &t2,
		ProgressGroup: &pb.ProgressGroup{Id: "g1"},
	})
	env.flush(t, "v2")

	sub := env.vertexWriter(d1)
	require.NoError(t, sub.Write("l1", client.VertexLog{Data: []byte("x")}))
	env.flush(t, "l1")
	_ = sub.Close()
	sub2 := env.vertexWriter(d2)
	require.NoError(t, sub2.Write("l2", client.VertexLog{Data: []byte("y")}))
	env.flush(t, "l2")
	_ = sub2.Close()

	ch := make(chan *client.SolveStatus, 64)
	go func() {
		_ = env.j.Status(t.Context(), ch, client.StatusOptions{
			Snapshot: true,
			Filters: []client.StatusFilter{{
				Vertexes: []string{d2.String()},
			}},
		})
	}()
	coll := collectStatus(ch)

	snapshot := coll.snapshot(t)
	require.Len(t, snapshot.Vertexes, 1)
	require.Equal(t, d2, snapshot.Vertexes[0].Digest)

	env.close()
	<-coll.closed
}

func TestJobStatusNoOptionsStreams(t *testing.T) {
	env := newStatusTestEnv(t)

	d1 := digest.FromString("v1")
	t1 := time.Now()
	env.write("v1", client.Vertex{Digest: d1, Name: "v1", Started: &t1})
	env.flush(t, "v1")

	ch := make(chan *client.SolveStatus, 64)
	go func() {
		_ = env.j.Status(t.Context(), ch)
	}()
	coll := collectStatus(ch)

	require.Eventually(t, func() bool {
		coll.mu.Lock()
		defer coll.mu.Unlock()
		return len(coll.all) > 0
	}, 5*time.Second, time.Millisecond)
	coll.mu.Lock()
	require.Len(t, coll.all[0].Vertexes, 1)
	require.Equal(t, d1, coll.all[0].Vertexes[0].Digest)
	require.False(t, coll.all[0].Snapshot)
	coll.mu.Unlock()

	env.close()
	<-coll.closed
}
