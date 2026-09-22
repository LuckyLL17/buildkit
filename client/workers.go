package client

import (
	"context"
	"time"

	controlapi "github.com/moby/buildkit/api/services/control"
	apitypes "github.com/moby/buildkit/api/types"
	"github.com/moby/buildkit/solver/pb"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

// WorkerInfo contains information about a worker
type WorkerInfo struct {
	ID              string              `json:"id"`
	Labels          map[string]string   `json:"labels"`
	Platforms       []ocispecs.Platform `json:"platforms"`
	GCPolicy        []PruneInfo         `json:"gcPolicy"`
	BuildkitVersion BuildkitVersion     `json:"buildkitVersion"`
	CDIDevices      []CDIDevice         `json:"cdiDevices"`
	// Capability is set when the query carried a CapabilityProfile.
	Capability *CapabilityMatch `json:"capability,omitempty"`
}

// CapabilityProfile describes the capabilities a worker must provide for a
// build. Every set field is a hard requirement; an empty profile matches
// every worker.
type CapabilityProfile struct {
	// Platforms the worker must support, all of them.
	Platforms []ocispecs.Platform `json:"platforms,omitempty"`
	// Labels the worker must carry. An empty value only requires the key to
	// be present, a non-empty value requires an exact match.
	Labels map[string]string `json:"labels,omitempty"`
	// CDIDevices the worker must provide, all of them. A name without an "="
	// matches every device of the same CDI kind.
	CDIDevices []string `json:"cdiDevices,omitempty"`
	// VersionConstraint is a comma-separated list of constraints against the
	// worker BuildKit version, e.g. ">=0.13.0,<1.0.0".
	VersionConstraint string `json:"versionConstraint,omitempty"`
}

// Empty reports whether the profile sets any requirement.
func (p CapabilityProfile) Empty() bool {
	return len(p.Platforms) == 0 &&
		len(p.Labels) == 0 &&
		len(p.CDIDevices) == 0 &&
		p.VersionConstraint == ""
}

// CapabilityMatch explains how a worker fits a CapabilityProfile.
type CapabilityMatch struct {
	// Compatible reports whether every requirement is satisfied.
	Compatible bool `json:"compatible"`
	// Rank orders compatible workers, higher ranks sort first.
	Rank int `json:"rank"`
	// Missing lists capabilities required by the profile but not provided.
	Missing []string `json:"missing,omitempty"`
	// Conflicts lists worker capabilities that contradict the profile.
	Conflicts []string `json:"conflicts,omitempty"`
}

// WorkerProfileResult is a capability selection result for one profile.
type WorkerProfileResult struct {
	Profile CapabilityProfile `json:"profile"`
	Workers []*WorkerInfo     `json:"workers"`
}

// ListWorkers lists all active workers.
// When a CapabilityProfile option is provided, workers are returned as
// stably sorted candidates carrying a Capability match result; without a
// profile the response keeps its original fields and order.
func (c *Client) ListWorkers(ctx context.Context, opts ...ListWorkersOption) ([]*WorkerInfo, error) {
	info := &ListWorkersInfo{}
	for _, o := range opts {
		o.SetListWorkersOption(info)
	}

	req := &controlapi.ListWorkersRequest{Filter: info.Filter}
	if info.Profile != nil {
		req.Profile = toAPICapabilityProfile(info.Profile)
	}
	resp, err := c.ControlClient().ListWorkers(ctx, req)
	if err != nil {
		return nil, errors.Wrap(err, "failed to list workers")
	}

	var wi []*WorkerInfo

	for _, w := range resp.Record {
		wi = append(wi, &WorkerInfo{
			ID:              w.ID,
			Labels:          w.Labels,
			Platforms:       pb.ToSpecPlatforms(w.Platforms),
			GCPolicy:        fromAPIGCPolicy(w.GCPolicy),
			BuildkitVersion: fromAPIBuildkitVersion(w.BuildkitVersion),
			CDIDevices:      fromAPICDIDevices(w.CDIDevices),
			Capability:      fromAPIWorkerCapability(w.Capability),
		})
	}

	return wi, nil
}

// SelectWorkers evaluates multiple capability profiles concurrently. The
// returned results preserve the order of the input profiles. Common query
// options, such as WithFilter, apply to every selection.
func (c *Client) SelectWorkers(ctx context.Context, profiles []CapabilityProfile, opts ...ListWorkersOption) ([]*WorkerProfileResult, error) {
	results := make([]*WorkerProfileResult, len(profiles))
	g, gctx := errgroup.WithContext(ctx)
	for i, p := range profiles {
		i, p := i, p
		g.Go(func() error {
			selectionOpts := make([]ListWorkersOption, 0, len(opts)+1)
			selectionOpts = append(selectionOpts, opts...)
			selectionOpts = append(selectionOpts, WithCapabilityProfile(p))
			workers, err := c.ListWorkers(gctx, selectionOpts...)
			if err != nil {
				return errors.Wrapf(err, "failed to select workers for profile %d", i+1)
			}
			results[i] = &WorkerProfileResult{Profile: p, Workers: workers}
			return nil
		})
	}
	if err := g.Wait(); err != nil {
		return nil, err
	}
	return results, nil
}

// ListWorkersOption is an option for a worker list query
type ListWorkersOption interface {
	SetListWorkersOption(*ListWorkersInfo)
}

// ListWorkersInfo is a payload for worker list query
type ListWorkersInfo struct {
	Filter  []string
	Profile *CapabilityProfile
}

// WithCapabilityProfile selects workers capable of the requested build.
func WithCapabilityProfile(profile CapabilityProfile) ListWorkersOption {
	return &capabilityProfileOption{profile: profile}
}

type capabilityProfileOption struct {
	profile CapabilityProfile
}

func (o *capabilityProfileOption) SetListWorkersOption(info *ListWorkersInfo) {
	p := o.profile
	info.Profile = &p
}

func toAPICapabilityProfile(in *CapabilityProfile) *apitypes.CapabilityProfile {
	if in == nil {
		return nil
	}
	return &apitypes.CapabilityProfile{
		Platforms:         pb.PlatformsFromSpec(in.Platforms),
		Labels:            in.Labels,
		CDIDevices:        in.CDIDevices,
		VersionConstraint: in.VersionConstraint,
	}
}

func fromAPIWorkerCapability(in *apitypes.WorkerCapability) *CapabilityMatch {
	if in == nil {
		return nil
	}
	return &CapabilityMatch{
		Compatible: in.Compatible,
		Rank:       int(in.Rank),
		Missing:    in.Missing,
		Conflicts:  in.Conflicts,
	}
}

func fromAPIGCPolicy(in []*apitypes.GCPolicy) []PruneInfo {
	out := make([]PruneInfo, 0, len(in))
	for _, p := range in {
		out = append(out, PruneInfo{
			All:           p.All,
			Filter:        p.Filters,
			KeepDuration:  time.Duration(p.KeepDuration),
			ReservedSpace: p.ReservedSpace,
			MaxUsedSpace:  p.MaxUsedSpace,
			MinFreeSpace:  p.MinFreeSpace,
		})
	}
	return out
}
