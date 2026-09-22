package debug

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"text/tabwriter"

	"github.com/containerd/platforms"
	"github.com/moby/buildkit/client"
	bccommon "github.com/moby/buildkit/cmd/buildctl/common"
	"github.com/moby/buildkit/util/bklog"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	"github.com/tonistiigi/units"
	"github.com/urfave/cli/v3"
)

var WorkersCommand = &cli.Command{
	Name:   "workers",
	Usage:  "list workers",
	Action: commandAction(listWorkers),
	Flags: []cli.Flag{
		&cli.StringSliceFlag{
			Name:    "filter",
			Aliases: []string{"f"},
			Usage:   "containerd-style filter string slice",
		},
		&cli.BoolFlag{
			Name:    "verbose",
			Aliases: []string{"v"},
			Usage:   "Verbose output",
		},
		&cli.StringFlag{
			Name:  "format",
			Usage: "Format the output using the given Go template, e.g, '{{json .}}'",
		},
		&cli.StringSliceFlag{
			Name:  "platform",
			Usage: "Capability selection: platform the worker must support, e.g. linux/arm64 (can be repeated)",
		},
		&cli.StringSliceFlag{
			Name:  "label",
			Usage: "Capability selection: required worker label key=value, empty value requires the key (can be repeated), e.g. org.mobyproject.buildkit.worker.snapshotter=overlay",
		},
		&cli.StringSliceFlag{
			Name:  "cdi-device",
			Usage: "Capability selection: CDI device the worker must provide, e.g. vendor.com/gpu=0 (can be repeated)",
		},
		&cli.StringFlag{
			Name:  "require-version",
			Usage: "Capability selection: BuildKit version constraint, e.g. '>=0.13.0,<1.0.0'",
		},
		&cli.StringSliceFlag{
			Name:  "profile",
			Usage: "Capability selection profile as JSON; can be repeated to run multiple selections concurrently",
		},
	},
}

// profileSpec is the JSON form of a capability profile accepted by --profile.
type profileSpec struct {
	Platforms         []string          `json:"platforms"`
	Labels            map[string]string `json:"labels"`
	CDIDevices        []string          `json:"cdiDevices"`
	VersionConstraint string            `json:"versionConstraint"`
}

func listWorkers(clicontext *cli.Command) error {
	c, err := bccommon.ResolveClient(clicontext)
	if err != nil {
		return err
	}

	profiles, err := capabilityProfiles(clicontext)
	if err != nil {
		return err
	}

	ctx := commandContext(clicontext)
	listOpts := []client.ListWorkersOption{client.WithFilter(clicontext.StringSlice("filter"))}

	if len(profiles) == 0 {
		workers, err := c.ListWorkers(ctx, listOpts...)
		if err != nil {
			return err
		}
		return renderWorkers(clicontext, func(verbose bool, tw *tabwriter.Writer) {
			if verbose {
				printWorkersVerbose(tw, workers)
			} else {
				printWorkersTable(tw, workers)
			}
		}, workers)
	}

	if len(profiles) == 1 {
		workers, err := c.ListWorkers(ctx, append(listOpts, client.WithCapabilityProfile(profiles[0]))...)
		if err != nil {
			return err
		}
		return renderWorkers(clicontext, func(verbose bool, tw *tabwriter.Writer) {
			fmt.Fprintf(tw, "Profile:\t%s\n", formatCapabilityProfile(profiles[0]))
			if verbose {
				printWorkersVerbose(tw, workers)
			} else {
				printSelectionTable(tw, workers)
			}
		}, workers)
	}

	results, err := c.SelectWorkers(ctx, profiles, listOpts...)
	if err != nil {
		return err
	}
	return renderWorkers(clicontext, func(verbose bool, tw *tabwriter.Writer) {
		for i, res := range results {
			fmt.Fprintf(tw, "Profile #%d:\t%s\n", i+1, formatCapabilityProfile(res.Profile))
			if verbose {
				printWorkersVerbose(tw, res.Workers)
			} else {
				printSelectionTable(tw, res.Workers)
			}
			fmt.Fprintln(tw)
		}
		tw.Flush()
	}, results)
}

// renderWorkers executes the format template on data when set, otherwise the
// provided table renderer.
func renderWorkers(clicontext *cli.Command, renderTable func(verbose bool, tw *tabwriter.Writer), data any) error {
	if format := clicontext.String("format"); format != "" {
		if clicontext.Bool("verbose") {
			bklog.L.Debug("Ignoring --verbose")
		}
		tmpl, err := bccommon.ParseTemplate(format)
		if err != nil {
			return err
		}
		if err := tmpl.Execute(clicontext.Root().Writer, data); err != nil {
			return err
		}
		_, err = fmt.Fprint(clicontext.Root().Writer, "\n")
		return err
	}

	tw := tabwriter.NewWriter(os.Stdout, 1, 8, 1, '\t', 0)
	renderTable(clicontext.Bool("verbose"), tw)
	return nil
}

func capabilityProfiles(clicontext *cli.Command) ([]client.CapabilityProfile, error) {
	var rawProfiles []string
	if p := clicontext.StringSlice("profile"); len(p) > 0 {
		rawProfiles = p
	}

	typedFlags := len(clicontext.StringSlice("platform")) > 0 ||
		len(clicontext.StringSlice("label")) > 0 ||
		len(clicontext.StringSlice("cdi-device")) > 0 ||
		clicontext.String("require-version") != ""

	if len(rawProfiles) > 0 && typedFlags {
		return nil, errors.New("--profile cannot be combined with --platform, --label, --cdi-device or --require-version")
	}

	if len(rawProfiles) > 0 {
		profiles := make([]client.CapabilityProfile, 0, len(rawProfiles))
		for _, raw := range rawProfiles {
			var spec profileSpec
			if err := json.Unmarshal([]byte(raw), &spec); err != nil {
				return nil, errors.Wrap(err, "invalid --profile JSON")
			}
			profile, err := newCapabilityProfile(spec.Platforms, spec.Labels, spec.CDIDevices, spec.VersionConstraint)
			if err != nil {
				return nil, err
			}
			profiles = append(profiles, profile)
		}
		return profiles, nil
	}

	if !typedFlags {
		return nil, nil
	}

	labels, err := parseLabelRequirements(clicontext.StringSlice("label"))
	if err != nil {
		return nil, err
	}
	profile, err := newCapabilityProfile(
		clicontext.StringSlice("platform"),
		labels,
		clicontext.StringSlice("cdi-device"),
		clicontext.String("require-version"),
	)
	if err != nil {
		return nil, err
	}
	return []client.CapabilityProfile{profile}, nil
}

func newCapabilityProfile(platformsArg []string, labels map[string]string, cdiDevices []string, versionConstraint string) (client.CapabilityProfile, error) {
	platformsList := make([]ocispecs.Platform, 0, len(platformsArg))
	for _, p := range platformsArg {
		platform, err := platforms.Parse(p)
		if err != nil {
			return client.CapabilityProfile{}, errors.Wrapf(err, "invalid platform %q", p)
		}
		platformsList = append(platformsList, platforms.Normalize(platform))
	}
	return client.CapabilityProfile{
		Platforms:         platformsList,
		Labels:            labels,
		CDIDevices:        cdiDevices,
		VersionConstraint: versionConstraint,
	}, nil
}

func parseLabelRequirements(labelArgs []string) (map[string]string, error) {
	if len(labelArgs) == 0 {
		return nil, nil
	}
	labels := make(map[string]string, len(labelArgs))
	for _, label := range labelArgs {
		key, value, ok := strings.Cut(label, "=")
		if key == "" {
			return nil, errors.Errorf("invalid label %q: missing key", label)
		}
		if !ok {
			value = ""
		}
		labels[key] = value
	}
	return labels, nil
}

func formatCapabilityProfile(p client.CapabilityProfile) string {
	var parts []string
	if len(p.Platforms) > 0 {
		formatted := make([]string, 0, len(p.Platforms))
		for _, plat := range p.Platforms {
			formatted = append(formatted, platforms.Format(plat))
		}
		parts = append(parts, "platforms="+strings.Join(formatted, ","))
	}
	if len(p.Labels) > 0 {
		formatted := make([]string, 0, len(p.Labels))
		for _, key := range sortedKeys(p.Labels) {
			formatted = append(formatted, key+"="+p.Labels[key])
		}
		parts = append(parts, "labels="+strings.Join(formatted, ","))
	}
	if len(p.CDIDevices) > 0 {
		parts = append(parts, "cdi-devices="+strings.Join(p.CDIDevices, ","))
	}
	if p.VersionConstraint != "" {
		parts = append(parts, "version="+p.VersionConstraint)
	}
	return strings.Join(parts, " ")
}

func printWorkersVerbose(tw *tabwriter.Writer, winfo []*client.WorkerInfo) {
	for _, wi := range winfo {
		fmt.Fprintf(tw, "ID:\t%s\n", wi.ID)
		fmt.Fprintf(tw, "Platforms:\t%s\n", joinPlatforms(wi.Platforms))
		fmt.Fprintf(tw, "BuildKit:\t%s %s %s\n", wi.BuildkitVersion.Package, wi.BuildkitVersion.Version, wi.BuildkitVersion.Revision)
		if wi.BuildkitVersion.DockerfileVersion != "" {
			fmt.Fprintf(tw, "Dockerfile:\t%s\n", wi.BuildkitVersion.DockerfileVersion)
		}
		printCapabilityVerbose(tw, wi.Capability)
		fmt.Fprint(tw, "Labels:\n")
		for _, k := range sortedKeys(wi.Labels) {
			v := wi.Labels[k]
			fmt.Fprintf(tw, "\t%s:\t%s\n", k, v)
		}
		if len(wi.CDIDevices) > 0 {
			fmt.Fprint(tw, "Devices:\n")
			for _, d := range wi.CDIDevices {
				fmt.Fprintf(tw, "\tName:\t%s\n", d.Name)
				if d.OnDemand {
					fmt.Fprintf(tw, "\tOnDemand:\t%v\n", d.OnDemand)
				} else {
					fmt.Fprintf(tw, "\tAutoAllow:\t%v\n", d.AutoAllow)
				}

				for _, k := range sortedKeys(d.Annotations) {
					v := d.Annotations[k]
					fmt.Fprintf(tw, "\tAnnotations:\t%s:\t%s\n", k, v)
				}
			}
			fmt.Fprint(tw, "\n")
		}
		for i, rule := range wi.GCPolicy {
			fmt.Fprintf(tw, "GC Policy rule#%d:\n", i)
			fmt.Fprintf(tw, "\tAll:\t%v\n", rule.All)
			if len(rule.Filter) > 0 {
				fmt.Fprintf(tw, "\tFilters:\t%s\n", strings.Join(rule.Filter, " "))
			}
			if rule.KeepDuration > 0 {
				fmt.Fprintf(tw, "\tKeep duration:\t%v\n", rule.KeepDuration.String())
			}
			if rule.ReservedSpace > 0 {
				fmt.Fprintf(tw, "\tReserved space:\t%g\n", units.Bytes(rule.ReservedSpace))
			}
			if rule.MinFreeSpace > 0 {
				fmt.Fprintf(tw, "\tMinimum free space:\t%g\n", units.Bytes(rule.MinFreeSpace))
			}
			if rule.MaxUsedSpace > 0 {
				fmt.Fprintf(tw, "\tMaximum used space:\t%g\n", units.Bytes(rule.MaxUsedSpace))
			}
		}
		fmt.Fprint(tw, "\n")
	}

	tw.Flush()
}

func printCapabilityVerbose(tw *tabwriter.Writer, capability *client.CapabilityMatch) {
	if capability == nil {
		return
	}
	fmt.Fprintf(tw, "Compatible:\t%v\n", capability.Compatible)
	fmt.Fprintf(tw, "Rank:\t%d\n", capability.Rank)
	for _, reason := range capability.Missing {
		fmt.Fprintf(tw, "Missing:\t%s\n", reason)
	}
	for _, reason := range capability.Conflicts {
		fmt.Fprintf(tw, "Conflict:\t%s\n", reason)
	}
}

func printWorkersTable(tw *tabwriter.Writer, winfo []*client.WorkerInfo) {
	fmt.Fprintln(tw, "ID\tPLATFORMS")

	for _, wi := range winfo {
		id := wi.ID
		fmt.Fprintf(tw, "%s\t%s\n", id, joinPlatforms(wi.Platforms))
	}

	tw.Flush()
}

func printSelectionTable(tw *tabwriter.Writer, winfo []*client.WorkerInfo) {
	fmt.Fprintln(tw, "ID\tPLATFORMS\tCOMPATIBLE\tRANK")

	for _, wi := range winfo {
		fmt.Fprintf(tw, "%s\t%s\t%v\t%d\n", wi.ID, joinPlatforms(wi.Platforms), wi.Capability.Compatible, wi.Capability.Rank)
	}

	rejected := false
	for _, wi := range winfo {
		if wi.Capability.Compatible {
			continue
		}
		if !rejected {
			fmt.Fprintln(tw, "\nRejected:\t")
			rejected = true
		}
		fmt.Fprintf(tw, "%s\t", wi.ID)
		var reasons []string
		for _, reason := range wi.Capability.Missing {
			reasons = append(reasons, "missing "+reason)
		}
		for _, reason := range wi.Capability.Conflicts {
			reasons = append(reasons, "conflict "+reason)
		}
		fmt.Fprintf(tw, "%s\n", strings.Join(reasons, "; "))
	}

	tw.Flush()
}

func sortedKeys[T any](m map[string]T) []string {
	return slices.Sorted(maps.Keys(m))
}

func commandContext(c *cli.Command) context.Context {
	return c.Root().Metadata["context"].(context.Context)
}

func joinPlatforms(p []ocispecs.Platform) string {
	str := make([]string, 0, len(p))
	for _, pp := range p {
		str = append(str, platforms.Format(platforms.Normalize(pp)))
	}
	return strings.Join(str, ",")
}
