package debug

import (
	"fmt"
	"os"

	controlapi "github.com/moby/buildkit/api/services/control"
	"github.com/moby/buildkit/client"
	bccommon "github.com/moby/buildkit/cmd/buildctl/common"
	"github.com/moby/buildkit/solver/pb"
	"github.com/moby/buildkit/util/appcontext"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
)

var MonitorCommand = &cli.Command{
	Name:   "monitor",
	Usage:  "display build events",
	Action: commandAction(monitor),
	Flags: []cli.Flag{
		&cli.BoolFlag{
			Name:  "completed",
			Usage: "show completed builds",
		},
		&cli.StringFlag{
			Name:  "ref",
			Usage: "show events for a specific build",
		},
		&cli.StringSliceFlag{
			Name:  "vertex",
			Usage: "filter status events by vertex digest or name (can be repeated)",
		},
		&cli.StringFlag{
			Name:  "progress-group",
			Usage: "filter status events by progress group ID",
		},
		&cli.StringSliceFlag{
			Name:  "event",
			Usage: "filter status events by type: vertex, status, log, warning (can be repeated)",
		},
		&cli.BoolFlag{
			Name:  "snapshot",
			Usage: "send the current build state as an initial snapshot before incremental events",
		},
	},
}

func monitor(clicontext *cli.Command) error {
	if hasStatusFilters(clicontext) {
		return monitorStatus(clicontext)
	}
	return monitorHistory(clicontext)
}

func hasStatusFilters(clicontext *cli.Command) bool {
	return clicontext.Bool("snapshot") ||
		len(clicontext.StringSlice("vertex")) > 0 ||
		clicontext.String("progress-group") != "" ||
		len(clicontext.StringSlice("event")) > 0
}

func monitorHistory(clicontext *cli.Command) error {
	c, err := bccommon.ResolveClient(clicontext)
	if err != nil {
		return err
	}
	completed := clicontext.Bool("completed")

	ctx := appcontext.Context()

	cl, err := c.ControlClient().ListenBuildHistory(ctx, &controlapi.BuildHistoryRequest{
		ActiveOnly: !completed,
		Ref:        clicontext.String("ref"),
	})
	if err != nil {
		return err
	}

	for {
		ev, err := cl.Recv()
		if err != nil {
			return err
		}
		fmt.Printf("event: %s ref:%s\n", ev.Type.String(), ev.Record.Ref)
		if ev.Record.NumTotalSteps != 0 {
			fmt.Printf("  cache: %d/%d\n", ev.Record.NumCachedSteps, ev.Record.NumTotalSteps)
		}
		if ev.Record.NumWarnings != 0 {
			fmt.Printf("  warnings: %d\n", ev.Record.NumWarnings)
		}
		if ev.Record.Logs != nil {
			fmt.Printf("  logs: %s\n", ev.Record.Logs)
		}
		if ev.Record.Trace != nil {
			fmt.Printf("  trace: %s\n", ev.Record.Trace)
		}

		if ev.Record.Result != nil {
			if len(ev.Record.Result.Results) > 0 {
				for _, res := range ev.Record.Result.Results {
					fmt.Printf("  descriptor: %s\n", res)
				}
			} else if ev.Record.Result.ResultDeprecated != nil {
				fmt.Printf("  descriptor: %s\n", ev.Record.Result.ResultDeprecated)
			}
			for _, att := range ev.Record.Result.Attestations {
				fmt.Printf("  attestation: %s\n", att)
			}
		}
		for k, res := range ev.Record.Results {
			if len(ev.Record.Result.Results) > 0 {
				for _, res := range res.Results {
					fmt.Printf("  [%s] descriptor: %s\n", k, res)
				}
			} else if res.ResultDeprecated != nil {
				fmt.Printf("  [%s] descriptor: %s\n", k, res.ResultDeprecated)
			}
			for _, att := range res.Attestations {
				fmt.Printf("  [%s] attestation: %s\n", k, att)
			}
		}
	}
}

func monitorStatus(clicontext *cli.Command) error {
	ref := clicontext.String("ref")
	if ref == "" {
		return errors.New("build ref must be specified with --ref when using status filters")
	}

	var types []client.StatusEventType
	for _, t := range clicontext.StringSlice("event") {
		et, err := parseStatusEventType(t)
		if err != nil {
			return err
		}
		types = append(types, et)
	}

	f := client.StatusFilter{
		Vertexes: clicontext.StringSlice("vertex"),
		Types:    types,
	}
	if group := clicontext.String("progress-group"); group != "" {
		f.ProgressGroup = &pb.ProgressGroup{Id: group}
	}
	if clicontext.Bool("completed") {
		completed := true
		f.Completed = &completed
	}

	opts := client.StatusOptions{
		Filters:  []client.StatusFilter{f},
		Snapshot: clicontext.Bool("snapshot"),
	}

	c, err := bccommon.ResolveClient(clicontext)
	if err != nil {
		return err
	}

	ctx := appcontext.Context()
	ch := make(chan *client.SolveStatus, 8)

	errCh := make(chan error, 1)
	go func() {
		errCh <- c.Status(ctx, ref, ch, opts)
	}()

	printedSnapshotHeader := false
	afterSnapshot := !opts.Snapshot
	for ss := range ch {
		if ss.Snapshot {
			if !printedSnapshotHeader {
				fmt.Println("snapshot:")
				printedSnapshotHeader = true
			}
			printSolveStatus("  ", ss)
			continue
		}
		if opts.Snapshot && !afterSnapshot {
			fmt.Println("events:")
			afterSnapshot = true
		}
		printSolveStatus("  ", ss)
	}
	return <-errCh
}

func parseStatusEventType(t string) (client.StatusEventType, error) {
	switch t {
	case "vertex":
		return client.StatusEventVertex, nil
	case "status":
		return client.StatusEventStatus, nil
	case "log":
		return client.StatusEventLog, nil
	case "warning":
		return client.StatusEventWarning, nil
	default:
		return 0, errors.Errorf("unknown event type %q (supported: vertex, status, log, warning)", t)
	}
}

func printSolveStatus(prefix string, ss *client.SolveStatus) {
	for _, v := range ss.Vertexes {
		state := "running"
		switch {
		case v.Error != "":
			state = "error: " + v.Error
		case v.Cached:
			state = "cached"
		case v.Completed != nil:
			state = "completed"
		case v.Started != nil:
			state = "started"
		}
		group := ""
		if v.ProgressGroup != nil {
			group = " group:" + v.ProgressGroup.Id
		}
		fmt.Printf("%svertex: %s name:%q%s %s\n", prefix, v.Digest, v.Name, group, state)
	}
	for _, st := range ss.Statuses {
		fmt.Printf("%sstatus: %s vertex:%s %d/%d\n", prefix, st.Name, st.Vertex, st.Current, st.Total)
	}
	for _, l := range ss.Logs {
		fmt.Printf("%slog: %s stream:%d ", prefix, l.Vertex, l.Stream)
		os.Stdout.Write(l.Data)
		if len(l.Data) == 0 || l.Data[len(l.Data)-1] != '\n' {
			fmt.Println()
		}
	}
	for _, w := range ss.Warnings {
		fmt.Printf("%swarning: %s level:%d %s\n", prefix, w.Vertex, w.Level, string(w.Short))
	}
}
