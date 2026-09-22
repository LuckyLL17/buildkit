package client

import (
	"slices"

	controlapi "github.com/moby/buildkit/api/services/control"
	"github.com/moby/buildkit/solver/pb"
	digest "github.com/opencontainers/go-digest"
)

// StatusEventType identifies a kind of SolveStatus event.
type StatusEventType int32

const (
	StatusEventVertex  StatusEventType = 1
	StatusEventStatus  StatusEventType = 2
	StatusEventLog     StatusEventType = 3
	StatusEventWarning StatusEventType = 4
)

// StatusFilter restricts the events sent on a Status stream. The zero value
// does not restrict the stream.
type StatusFilter struct {
	// Vertexes matches vertexes by their digest or exact name. Status, log
	// and warning events referencing a matched vertex are included as well.
	Vertexes []string
	// ProgressGroup matches vertexes belonging to the given progress group.
	ProgressGroup *pb.ProgressGroup
	// Types restricts the event types that are sent. When unset, all event
	// types are sent.
	Types []StatusEventType
	// Completed selects vertexes by their completion state: nil matches any,
	// true matches completed vertexes and false matches running ones.
	Completed *bool
}

// StatusOptions configures a Status stream. The zero value preserves the
// default streaming behavior.
type StatusOptions struct {
	// Filters are alternatives: an event is sent if it matches any of the
	// filters. When empty, every event is sent.
	Filters []StatusFilter
	// Snapshot sends the current build state as an ordered initial snapshot
	// before continuing with the incremental events.
	Snapshot bool
}

func StatusOptionsFromPB(req *controlapi.StatusRequest) StatusOptions {
	return StatusOptions{
		Filters:  StatusFiltersFromPB(req.Filters),
		Snapshot: req.Snapshot,
	}
}

func (o StatusOptions) ToPBFilters() []*controlapi.StatusFilter {
	return StatusFiltersToPB(o.Filters)
}

func StatusFiltersFromPB(in []*controlapi.StatusFilter) []StatusFilter {
	if len(in) == 0 {
		return nil
	}
	out := make([]StatusFilter, 0, len(in))
	for _, f := range in {
		if f == nil {
			continue
		}
		var types []StatusEventType
		for _, t := range f.Types {
			types = append(types, StatusEventType(t))
		}
		out = append(out, StatusFilter{
			Vertexes:      slices.Clone(f.Vertexes),
			ProgressGroup: f.ProgressGroup,
			Types:         types,
			Completed:     completionFromPB(f.Completed),
		})
	}
	return out
}

func StatusFiltersToPB(in []StatusFilter) []*controlapi.StatusFilter {
	if len(in) == 0 {
		return nil
	}
	out := make([]*controlapi.StatusFilter, 0, len(in))
	for _, f := range in {
		types := make([]controlapi.StatusEventType, 0, len(f.Types))
		for _, t := range f.Types {
			types = append(types, controlapi.StatusEventType(t))
		}
		out = append(out, &controlapi.StatusFilter{
			Vertexes:      slices.Clone(f.Vertexes),
			ProgressGroup: f.ProgressGroup,
			Types:         types,
			Completed:     completionToPB(f.Completed),
		})
	}
	return out
}

func completionToPB(completed *bool) controlapi.StatusCompletion {
	if completed == nil {
		return controlapi.StatusCompletion_COMPLETION_ANY
	}
	if *completed {
		return controlapi.StatusCompletion_COMPLETION_COMPLETED
	}
	return controlapi.StatusCompletion_COMPLETION_RUNNING
}

func completionFromPB(c controlapi.StatusCompletion) *bool {
	switch c {
	case controlapi.StatusCompletion_COMPLETION_COMPLETED:
		v := true
		return &v
	case controlapi.StatusCompletion_COMPLETION_RUNNING:
		v := false
		return &v
	default:
		return nil
	}
}

type normalizedStatusFilter struct {
	digests   map[digest.Digest]struct{}
	names     map[string]struct{}
	group     string
	types     map[StatusEventType]struct{}
	completed *bool
}

// StatusProcessor maintains the current state of a status stream and filters
// the events that pass through it. It is not safe for concurrent use.
type StatusProcessor struct {
	filters  []normalizedStatusFilter
	vertexes map[digest.Digest]*Vertex
	statuses map[string]*VertexStatus
	warnings []*VertexWarning
}

func NewStatusProcessor(opts StatusOptions) *StatusProcessor {
	p := &StatusProcessor{
		vertexes: map[digest.Digest]*Vertex{},
		statuses: map[string]*VertexStatus{},
	}
	for _, f := range opts.Filters {
		nf := normalizedStatusFilter{group: progressGroupID(f.ProgressGroup)}
		for _, v := range f.Vertexes {
			if dgst, err := digest.Parse(v); err == nil {
				if nf.digests == nil {
					nf.digests = map[digest.Digest]struct{}{}
				}
				nf.digests[dgst] = struct{}{}
			} else {
				if nf.names == nil {
					nf.names = map[string]struct{}{}
				}
				nf.names[v] = struct{}{}
			}
		}
		if len(f.Types) > 0 {
			nf.types = map[StatusEventType]struct{}{}
			for _, t := range f.Types {
				nf.types[t] = struct{}{}
			}
		}
		nf.completed = f.Completed
		p.filters = append(p.filters, nf)
	}
	return p
}

func progressGroupID(pg *pb.ProgressGroup) string {
	if pg == nil {
		return ""
	}
	return pg.Id
}

// Apply records the state carried by ss and returns a copy containing only
// the events that match the filters. nil is returned when no event matches.
// Snapshot marker messages are passed through unchanged.
func (p *StatusProcessor) Apply(ss *SolveStatus) *SolveStatus {
	if ss == nil {
		return nil
	}
	if len(ss.Vertexes) == 0 && len(ss.Statuses) == 0 && len(ss.Logs) == 0 && len(ss.Warnings) == 0 {
		return ss
	}
	out := &SolveStatus{Snapshot: ss.Snapshot}
	for _, v := range ss.Vertexes {
		if v == nil {
			continue
		}
		p.vertexes[v.Digest] = v
		if p.match(StatusEventVertex, v.Digest, v) {
			out.Vertexes = append(out.Vertexes, v)
		}
	}
	for _, st := range ss.Statuses {
		if st == nil {
			continue
		}
		p.statuses[st.ID] = st
		if p.match(StatusEventStatus, st.Vertex, nil) {
			out.Statuses = append(out.Statuses, st)
		}
	}
	for _, l := range ss.Logs {
		if l == nil {
			continue
		}
		if p.match(StatusEventLog, l.Vertex, nil) {
			out.Logs = append(out.Logs, l)
		}
	}
	for _, w := range ss.Warnings {
		if w == nil {
			continue
		}
		p.warnings = append(p.warnings, w)
		if p.match(StatusEventWarning, w.Vertex, nil) {
			out.Warnings = append(out.Warnings, w)
		}
	}
	if len(out.Vertexes) == 0 && len(out.Statuses) == 0 && len(out.Logs) == 0 && len(out.Warnings) == 0 {
		return nil
	}
	return out
}

// Snapshot returns the current state matching the filters as a single
// deterministically ordered snapshot.
func (p *StatusProcessor) Snapshot() *SolveStatus {
	ss := &SolveStatus{Snapshot: true}
	for _, v := range p.vertexes {
		if p.match(StatusEventVertex, v.Digest, v) {
			ss.Vertexes = append(ss.Vertexes, v)
		}
	}
	for _, st := range p.statuses {
		if p.match(StatusEventStatus, st.Vertex, nil) {
			ss.Statuses = append(ss.Statuses, st)
		}
	}
	for _, w := range p.warnings {
		if p.match(StatusEventWarning, w.Vertex, nil) {
			ss.Warnings = append(ss.Warnings, w)
		}
	}
	sortSnapshot(ss)
	return ss
}

// snapshotChunkLimit bounds the number of events in a single snapshot
// message so that large builds stay within gRPC message size limits.
const snapshotChunkLimit = 500

// SnapshotMessages splits an ordered snapshot into bounded messages,
// preserving order: vertexes first, then statuses, then warnings. Every
// returned message is marked as part of the snapshot. A nil result means the
// snapshot is empty.
func SnapshotMessages(ss *SolveStatus) []*SolveStatus {
	if ss == nil {
		return nil
	}
	chunk := func(vertexes []*Vertex, statuses []*VertexStatus, logs []*VertexLog, warnings []*VertexWarning) *SolveStatus {
		return &SolveStatus{
			Snapshot: true,
			Vertexes: vertexes,
			Statuses: statuses,
			Logs:     logs,
			Warnings: warnings,
		}
	}
	var out []*SolveStatus
	for i := 0; i < len(ss.Vertexes); i += snapshotChunkLimit {
		out = append(out, chunk(ss.Vertexes[i:min(i+snapshotChunkLimit, len(ss.Vertexes))], nil, nil, nil))
	}
	for i := 0; i < len(ss.Statuses); i += snapshotChunkLimit {
		out = append(out, chunk(nil, ss.Statuses[i:min(i+snapshotChunkLimit, len(ss.Statuses))], nil, nil))
	}
	for i := 0; i < len(ss.Warnings); i += snapshotChunkLimit {
		out = append(out, chunk(nil, nil, nil, ss.Warnings[i:min(i+snapshotChunkLimit, len(ss.Warnings))]))
	}
	return out
}

func sortSnapshot(ss *SolveStatus) {
	slices.SortFunc(ss.Vertexes, func(a, b *Vertex) int {
		if (a.Started == nil) != (b.Started == nil) {
			if a.Started == nil {
				return -1
			}
			return 1
		}
		if a.Started != nil {
			if c := a.Started.Compare(*b.Started); c != 0 {
				return c
			}
		}
		if a.Digest != b.Digest {
			if a.Digest < b.Digest {
				return -1
			}
			return 1
		}
		return 0
	})
	slices.SortFunc(ss.Statuses, func(a, b *VertexStatus) int {
		if c := a.Timestamp.Compare(b.Timestamp); c != 0 {
			return c
		}
		if a.ID < b.ID {
			return -1
		}
		if a.ID > b.ID {
			return 1
		}
		return 0
	})
	slices.SortFunc(ss.Warnings, func(a, b *VertexWarning) int {
		if a.Vertex != b.Vertex {
			if a.Vertex < b.Vertex {
				return -1
			}
			return 1
		}
		return slices.Compare(a.Short, b.Short)
	})
}

// match reports whether an event of the given type referencing dgst passes
// the filters. When the event itself is a vertex, its latest value is passed
// directly; otherwise the referenced vertex is looked up from the state.
func (p *StatusProcessor) match(t StatusEventType, dgst digest.Digest, vertex *Vertex) bool {
	if len(p.filters) == 0 {
		return true
	}
	for _, f := range p.filters {
		if f.matches(t, dgst, vertex, p.vertexes[dgst]) {
			return true
		}
	}
	return false
}

func (f *normalizedStatusFilter) matches(t StatusEventType, dgst digest.Digest, vertex, known *Vertex) bool {
	if len(f.types) > 0 {
		if _, ok := f.types[t]; !ok {
			return false
		}
	}
	if f.completed == nil && len(f.digests) == 0 && len(f.names) == 0 && f.group == "" {
		return true
	}
	if vertex == nil {
		vertex = known
	}
	if vertex == nil {
		return false
	}
	if f.completed != nil {
		if (vertex.Completed != nil) != *f.completed {
			return false
		}
	}
	if len(f.digests) == 0 && len(f.names) == 0 && f.group == "" {
		return true
	}
	if _, ok := f.digests[vertex.Digest]; ok {
		return true
	}
	if _, ok := f.names[vertex.Name]; ok {
		return true
	}
	if f.group != "" && vertex.ProgressGroup != nil && vertex.ProgressGroup.Id == f.group {
		return true
	}
	return false
}
