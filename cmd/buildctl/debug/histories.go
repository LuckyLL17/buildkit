package debug

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
	"time"

	controlapi "github.com/moby/buildkit/api/services/control"
	bccommon "github.com/moby/buildkit/cmd/buildctl/common"
	"github.com/moby/buildkit/util/appcontext"
	"github.com/pkg/errors"
	"github.com/urfave/cli/v3"
)

var HistoriesCommand = &cli.Command{
	Name:   "histories",
	Usage:  "list build records",
	Action: commandAction(histories),
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:  "format",
			Usage: "Format the output using the given Go template, e.g, '{{json .}}'",
		},
		&cli.StringFlag{
			Name:  "cursor",
			Usage: "Resume from a cursor returned by a previous call",
		},
	},
}

func histories(clicontext *cli.Command) error {
	c, err := bccommon.ResolveClient(clicontext)
	if err != nil {
		return err
	}

	ctx := appcontext.Context()
	resp, err := c.ControlClient().ListenBuildHistory(ctx, &controlapi.BuildHistoryRequest{
		EarlyExit: true,
		Cursor:    clicontext.String("cursor"),
	})
	if err != nil {
		return err
	}

	if format := clicontext.String("format"); format != "" {
		tmpl, err := bccommon.ParseTemplate(format)
		if err != nil {
			return err
		}
		for {
			ev, err := resp.Recv()
			if errors.Is(err, io.EOF) {
				break
			} else if err != nil {
				return err
			}
			if err := tmpl.Execute(clicontext.Root().Writer, ev); err != nil {
				return err
			}
			if _, err = fmt.Fprint(clicontext.Root().Writer, "\n"); err != nil {
				return err
			}
		}
		return nil
	}
	return printRecordsTable(clicontext.Root().Writer, resp)
}

func printRecordsTable(w io.Writer, eventReceiver controlapi.Control_ListenBuildHistoryClient) error {
	tw := tabwriter.NewWriter(w, 1, 8, 1, '\t', 0)
	fmt.Fprintln(tw, "TYPE\tREF\tCREATED\tCOMPLETED\tGENERATION\tPINNED")
	var (
		nextCursor string
		maxSeq     uint64
	)
	for {
		ev, err := eventReceiver.Recv()
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}
		if seq, ok := cursorSequence(ev.Cursor); ok && seq > maxSeq {
			maxSeq = seq
			nextCursor = ev.Cursor
		}
		var (
			ref         string
			createdAt   string
			completedAt string
			generation  int32
			pinned      string
		)
		if r := ev.Record; r != nil {
			ref = r.Ref
			if r.CreatedAt != nil {
				createdAt = r.CreatedAt.AsTime().Local().Format(time.RFC3339)
			}
			if r.CompletedAt != nil {
				completedAt = r.CompletedAt.AsTime().Local().Format(time.RFC3339)
			}
			generation = r.Generation
			if r.Pinned {
				pinned = "*"
			}
		}
		fmt.Fprintf(tw, "%s\t%s\t%s\t%s\t%d\t%s\n", ev.Type, ref, createdAt, completedAt, generation, pinned)
		tw.Flush()
	}
	if err := tw.Flush(); err != nil {
		return err
	}
	_, err := fmt.Fprintf(w, "next cursor: %s\n", nextCursor)
	return err
}

func cursorSequence(cursor string) (uint64, bool) {
	if !strings.HasPrefix(cursor, "bkh_") {
		return 0, false
	}
	dt, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(cursor, "bkh_"))
	if err != nil {
		return 0, false
	}
	var c struct {
		Seq uint64 `json:"seq"`
	}
	if err := json.Unmarshal(dt, &c); err != nil {
		return 0, false
	}
	return c.Seq, true
}
