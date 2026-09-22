package main

import (
	"fmt"
	"os"
	"text/tabwriter"
	"time"

	"github.com/moby/buildkit/client"
	bccommon "github.com/moby/buildkit/cmd/buildctl/common"
	"github.com/moby/buildkit/util/bklog"
	"github.com/tonistiigi/units"
	"github.com/urfave/cli/v3"
)

var pruneCommand = &cli.Command{
	Name:   "prune",
	Usage:  "clean up build cache",
	Action: commandAction(prune),
	Flags: []cli.Flag{
		&cli.DurationFlag{
			Name:  "keep-duration",
			Usage: "Keep data newer than this limit",
		},
		&cli.Float64Flag{
			Name:  "keep-storage",
			Usage: "Keep data below this limit (in MB)",
		},
		&cli.Float64Flag{
			Name:  "keep-storage-min",
			Usage: "Always allow data above this limit (in MB)",
		},
		&cli.Float64Flag{
			Name:  "free-storage",
			Usage: "Keep free data below this limit (in MB)",
		},
		&cli.StringSliceFlag{
			Name:    "filter",
			Aliases: []string{"f"},
			Usage:   "Filter records",
		},
		&cli.BoolFlag{
			Name:  "all",
			Usage: "Include internal/frontend references",
		},
		&cli.BoolFlag{
			Name:  "plan",
			Usage: "Show records that would be pruned without removing them",
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
	},
}

func prune(clicontext *cli.Command) error {
	c, err := bccommon.ResolveClient(clicontext)
	if err != nil {
		return err
	}

	plan := clicontext.Bool("plan")

	ch := make(chan client.UsageInfo)
	printed := make(chan struct{})
	var summarizer func()

	opts := []client.PruneOption{
		client.WithFilter(clicontext.StringSlice("filter")),
		client.WithKeepOpt(
			clicontext.Duration("keep-duration"),
			int64(clicontext.Float64("keep-storage-min")*1e6),
			int64(clicontext.Float64("keep-storage")*1e6),
			int64(clicontext.Float64("free-storage")*1e6),
		),
	}

	if clicontext.Bool("all") {
		opts = append(opts, client.PruneAll)
	}

	if plan {
		opts = append(opts, client.PrunePlan)
	}

	if format := clicontext.String("format"); format != "" {
		if clicontext.Bool("verbose") {
			bklog.L.Debug("Ignoring --verbose")
		}
		tmpl, err := bccommon.ParseTemplate(format)
		if err != nil {
			return err
		}
		go func() {
			defer close(printed)
			for du := range ch {
				// Unlike `buildctl du`, the template is applied to a UsageInfo, not to a slice of UsageInfo
				if err := tmpl.Execute(clicontext.Root().Writer, du); err != nil {
					panic(err)
				}
				if _, err = fmt.Fprint(clicontext.Root().Writer, "\n"); err != nil {
					panic(err)
				}
			}
		}()
	} else {
		if plan && clicontext.Bool("verbose") {
			bklog.L.Debug("Ignoring --verbose")
		}
		tw := tabwriter.NewWriter(os.Stdout, 1, 8, 1, '\t', 0)
		first := true
		total := int64(0)
		go func() {
			defer close(printed)
			for du := range ch {
				if plan {
					if du.Delete {
						total += du.Size
					}
					if first {
						printPlanTableHeader(tw)
						first = false
					}
					printPlanTableRow(tw, &du)
					tw.Flush()
					continue
				}
				total += du.Size
				if clicontext.Bool("verbose") {
					printVerbose(tw, []*client.UsageInfo{&du})
				} else {
					if first {
						printTableHeader(tw)
						first = false
					}
					printTableRow(tw, &du)
					tw.Flush()
				}
			}
		}()
		summarizer = func() {
			tw = tabwriter.NewWriter(os.Stdout, 1, 8, 1, '\t', 0)
			if plan {
				fmt.Fprintf(tw, "Total reclaimable:\t%.2f\n", units.Bytes(total))
			} else {
				fmt.Fprintf(tw, "Total:\t%.2f\n", units.Bytes(total))
			}
			tw.Flush()
		}
	}

	err = c.Prune(bccommon.CommandContext(clicontext), ch, opts...)
	close(ch)
	<-printed
	if err != nil {
		return err
	}
	if summarizer != nil {
		summarizer()
	}
	return nil
}

var pruneSkipReasonText = map[string]string{
	client.PruneSkipInUse:        "in use",
	client.PruneSkipType:         "record type excluded without --all",
	client.PruneSkipShared:       "shared record excluded without --all",
	client.PruneSkipKeepDuration: "within keep duration",
	client.PruneSkipFilter:       "does not match filter",
	client.PruneSkipCapacity:     "capacity threshold reached",
}

func printPlanTableHeader(tw *tabwriter.Writer) {
	fmt.Fprintln(tw, "ID\tRECLAIMABLE\tSIZE\tLAST ACCESSED\tACTION\tREASON")
}

func printPlanTableRow(tw *tabwriter.Writer, di *client.UsageInfo) {
	id := di.ID
	if di.Mutable {
		id += "*"
	}
	size := fmt.Sprintf("%.2f", units.Bytes(di.Size))
	if di.Shared {
		size += "*"
	}
	lastUsed := "-"
	if di.LastUsedAt != nil {
		lastUsed = di.LastUsedAt.Format(time.RFC3339)
	}
	action := "keep"
	reason := pruneSkipReasonText[di.Reason]
	if reason == "" {
		reason = di.Reason
	}
	if di.Delete {
		action = "remove"
		reason = ""
	}
	fmt.Fprintf(tw, "%-71s\t%-11v\t%s\t%s\t%s\t%s\t\n", id, !di.InUse, size, lastUsed, action, reason)
}
