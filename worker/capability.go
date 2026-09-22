package worker

import (
	"context"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/containerd/platforms"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/solver/llbsolver/cdidevices"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// Candidate pairs a worker with its capability match result.
type Candidate struct {
	Worker Worker
	Match  client.CapabilityMatch
}

// Select evaluates the profile against all registered workers concurrently
// and returns stably sorted candidates: compatible workers first (highest
// rank, then worker ID), followed by incompatible workers with their missing
// capabilities and conflicts explained.
func (c *Controller) Select(ctx context.Context, profile client.CapabilityProfile) ([]Candidate, error) {
	c.mu.RLock()
	workers := make([]Worker, len(c.workers))
	copy(workers, c.workers)
	c.mu.RUnlock()
	return MatchProfile(ctx, workers, profile)
}

// MatchProfile evaluates a profile against the given workers concurrently.
// It is useful for selections scoped to a pre-filtered set of workers. The
// returned order is deterministic and independent of scheduling order.
func MatchProfile(ctx context.Context, workers []Worker, profile client.CapabilityProfile) ([]Candidate, error) {
	cond, err := compileProfile(profile)
	if err != nil {
		return nil, err
	}

	candidates := make([]Candidate, len(workers))
	g, _ := errgroup.WithContext(ctx)
	for i, w := range workers {
		i, w := i, w
		g.Go(func() error {
			candidates[i] = Candidate{
				Worker: w,
				Match:  cond.match(snapshotWorker(w)),
			}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}

	sort.SliceStable(candidates, func(i, j int) bool {
		mi, mj := candidates[i].Match, candidates[j].Match
		if mi.Compatible != mj.Compatible {
			return mi.Compatible
		}
		if mi.Rank != mj.Rank {
			return mi.Rank > mj.Rank
		}
		return candidates[i].Worker.ID() < candidates[j].Worker.ID()
	})
	return candidates, nil
}

type workerSnapshot struct {
	id        string
	labels    map[string]string
	platforms []ocispecs.Platform
	version   client.BuildkitVersion
	devices   []cdidevices.Device
}

func snapshotWorker(w Worker) workerSnapshot {
	s := workerSnapshot{
		id:        w.ID(),
		labels:    w.Labels(),
		platforms: w.Platforms(true),
		version:   w.BuildkitVersion(),
	}
	if m := w.CDIManager(); m != nil {
		s.devices = m.ListDevices()
	}
	return s
}

type profileConditions struct {
	platforms []ocispecs.Platform
	labels    map[string]string
	cdiDevices []string
	versions  []versionConstraint
	raw       string
}

func compileProfile(profile client.CapabilityProfile) (profileConditions, error) {
	cond := profileConditions{
		labels:     profile.Labels,
		cdiDevices: append([]string(nil), profile.CDIDevices...),
		raw:        strings.TrimSpace(profile.VersionConstraint),
	}
	for _, p := range profile.Platforms {
		cond.platforms = append(cond.platforms, platforms.Normalize(p))
	}
	sort.Slice(cond.platforms, func(i, j int) bool {
		return platforms.Format(cond.platforms[i]) < platforms.Format(cond.platforms[j])
	})
	sort.Strings(cond.cdiDevices)
	if cond.raw != "" {
		vs, err := parseVersionConstraints(cond.raw)
		if err != nil {
			return cond, err
		}
		cond.versions = vs
	}
	return cond, nil
}

func (cond profileConditions) match(s workerSnapshot) client.CapabilityMatch {
	m := client.CapabilityMatch{Compatible: true}

	if len(cond.platforms) > 0 {
		for _, required := range cond.platforms {
			matcher := platforms.Only(required)
			found := false
			for _, p := range s.platforms {
				if matcher.Match(p) {
					found = true
					break
				}
			}
			if found {
				m.Rank += 3
				continue
			}
			m.Compatible = false
			m.Missing = append(m.Missing, fmt.Sprintf("platform %s is not supported", platforms.Format(required)))
		}

		for _, p := range s.platforms {
			covered := false
			for _, required := range cond.platforms {
				if platforms.Only(required).Match(p) {
					covered = true
					break
				}
			}
			if !covered {
				// Prefer specialized workers over generic ones when ranks tie.
				m.Rank--
			}
		}
	}

	for _, key := range sortedStringKeys(cond.labels) {
		expected := cond.labels[key]
		got, ok := s.labels[key]
		switch {
		case !ok:
			m.Compatible = false
			m.Missing = append(m.Missing, fmt.Sprintf("label %q is not set", key))
		case expected != "" && got != expected:
			m.Compatible = false
			m.Conflicts = append(m.Conflicts, fmt.Sprintf("label %q is %q, expected %q", key, got, expected))
		default:
			m.Rank++
		}
	}

	for _, required := range cond.cdiDevices {
		rank, ok := matchCDIDevice(required, s.devices)
		if !ok {
			m.Compatible = false
			m.Missing = append(m.Missing, fmt.Sprintf("cdi device %q is not available", required))
			continue
		}
		m.Rank += rank
	}

	if len(cond.versions) > 0 {
		v, err := parseVersion(s.version.Version)
		if err != nil {
			m.Compatible = false
			m.Conflicts = append(m.Conflicts, fmt.Sprintf("buildkit version %q cannot satisfy %q: %v", s.version.Version, cond.raw, err))
		} else {
			satisfied := true
			for _, vc := range cond.versions {
				if !vc.matches(v) {
					satisfied = false
					break
				}
			}
			if satisfied {
				m.Rank++
			} else {
				m.Compatible = false
				m.Conflicts = append(m.Conflicts, fmt.Sprintf("buildkit version %q does not satisfy %q", s.version.Version, cond.raw))
			}
		}
	}

	return m
}

// matchCDIDevice reports the match rank for a required CDI device.
// A full "vendor/class=device" name requires an exact match. A bare CDI kind
// ("vendor/class") matches an exact on-demand device name or any device of
// that kind; exact matches rank higher than kind matches.
func matchCDIDevice(required string, devices []cdidevices.Device) (int, bool) {
	kind, _, explicit := strings.Cut(required, "=")
	rank := 0
	for _, d := range devices {
		switch {
		case d.Name == required:
			return 2, true
		case !explicit:
			dKind, _, _ := strings.Cut(d.Name, "=")
			if dKind == kind {
				rank = 1
			}
		}
	}
	return rank, rank > 0
}

func sortedStringKeys(m map[string]string) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type versionConstraint struct {
	op  string
	ver []int
	raw string
}

func parseVersionConstraints(s string) ([]versionConstraint, error) {
	var out []versionConstraint
	for _, clause := range strings.Split(s, ",") {
		clause = strings.TrimSpace(clause)
		if clause == "" {
			return nil, errors.Errorf("invalid version constraint %q: empty clause", s)
		}
		op := "="
		rest := clause
		for _, candidate := range []string{">=", "<=", "==", "!=", "=", ">", "<"} {
			if strings.HasPrefix(clause, candidate) {
				op = candidate
				rest = strings.TrimSpace(strings.TrimPrefix(clause, candidate))
				break
			}
		}
		ver, err := parseVersion(rest)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid version constraint %q", s)
		}
		out = append(out, versionConstraint{op: op, ver: ver, raw: rest})
	}
	return out, nil
}

// parseVersion parses the leading dotted numeric core of a BuildKit version
// such as "v0.13.1+unknown". Build metadata and prerelease suffixes are
// ignored; missing components are treated as zero during comparison.
func parseVersion(s string) ([]int, error) {
	s = strings.TrimSpace(s)
	s = strings.TrimPrefix(s, "v")
	if i := strings.IndexAny(s, "-+"); i >= 0 {
		s = s[:i]
	}
	if s == "" {
		return nil, errors.New("empty version")
	}
	parts := strings.Split(s, ".")
	out := make([]int, len(parts))
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil {
			return nil, errors.Errorf("invalid version component %q", p)
		}
		out[i] = n
	}
	return out, nil
}

func compareVersions(a, b []int) int {
	n := len(a)
	if len(b) > n {
		n = len(b)
	}
	for i := 0; i < n; i++ {
		x, y := 0, 0
		if i < len(a) {
			x = a[i]
		}
		if i < len(b) {
			y = b[i]
		}
		if x != y {
			if x < y {
				return -1
			}
			return 1
		}
	}
	return 0
}

func (c versionConstraint) matches(v []int) bool {
	cmp := compareVersions(v, c.ver)
	switch c.op {
	case ">=":
		return cmp >= 0
	case "<=":
		return cmp <= 0
	case ">":
		return cmp > 0
	case "<":
		return cmp < 0
	case "!=":
		return cmp != 0
	default:
		return cmp == 0
	}
}
