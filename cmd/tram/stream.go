// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"text/tabwriter"

	"github.com/pmezard/go-difflib/difflib"
	"github.com/spf13/cobra"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/go-tram/tram/internal/adapters/api"
	"github.com/go-tram/tram/internal/adapters/configsync"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testrun"
	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

// errApplyWouldFail is what diff returns for a stream that applying would
// not build; diff writes the reason.
var errApplyWouldFail = errors.New("applying would fail")

// errWriteInKubernetes refuses changes to pods in Kubernetes: their streams
// come from Stream resources or a file sync, which would undo the change, and
// a change through the API reaches one pod only.
var errWriteInKubernetes = errors.New("streams of pods in Kubernetes come from Stream resources or the file sync, " +
	"not from the API: change those, e.g. with kubectl apply")

// diffContext is the number of unchanged lines around a change in a diff.
const diffContext = 3

func newStreamCmd() *cobra.Command {
	var target targetFlags
	cmd := &cobra.Command{
		Use:   "stream",
		Short: "List, get, apply, diff and delete the streams of a server",
		Long: `List, get, apply, diff and delete the streams of a TRAM server through its
admin API. Stream files are the files of the file sync and tram test: a bare
stream object or {"stream": {...}}; tram stream get writes one.

In Kubernetes, --engine or --selector read from one of the pods, which all
run the same streams; apply and delete are refused there, since the streams
come from Stream resources.`,
	}
	addTargetFlags(cmd, &target)
	cmd.AddCommand(
		newStreamListCmd(&target),
		newStreamGetCmd(&target),
		newStreamApplyCmd(&target),
		newStreamDiffCmd(&target),
		newStreamDeleteCmd(&target),
	)
	return cmd
}

// withStreams calls fn with a client of the StreamService of one instance of
// the target. write refuses a target in Kubernetes.
func withStreams(
	cmd *cobra.Command,
	target *targetFlags,
	write bool,
	fn func(context.Context, v1.StreamServiceClient) error,
) error {
	t, err := target.resolve()
	if err != nil {
		return err
	}
	if write && t.kube != nil {
		return errWriteInKubernetes
	}
	cmd.SilenceUsage = true
	c, err := t.connect(cmd.Context(), cmd.ErrOrStderr())
	if err != nil {
		return err
	}
	in, closeConn, err := c.one(cmd.Context())
	if err != nil {
		return err
	}
	defer closeConn()
	return fn(cmd.Context(), v1.NewStreamServiceClient(in.conn))
}

// streamError makes the status of a call readable.
func streamError(err error) error {
	st, ok := status.FromError(err)
	if !ok {
		return err
	}
	if st.Code() == codes.Unavailable {
		return fmt.Errorf("admin API not available: %s", st.Message())
	}
	return errors.New(st.Message())
}

func newStreamListCmd(target *targetFlags) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "list",
		Short: "List the streams",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if output != outputText && output != outputJSON {
				return fmt.Errorf("invalid --output %q: use %s or %s", output, outputText, outputJSON)
			}
			return withStreams(cmd, target, false, func(ctx context.Context, client v1.StreamServiceClient) error {
				resp, err := client.ListStreams(ctx, &v1.ListStreamsRequest{})
				if err != nil {
					return streamError(err)
				}
				if output == outputJSON {
					files := make([]*configsync.Stream, 0, len(resp.GetStreams()))
					for _, s := range resp.GetStreams() {
						files = append(files, streamFile(api.StreamFromPB(s)))
					}
					return writeIndented(cmd.OutOrStdout(), files)
				}
				return writeStreamTable(cmd.OutOrStdout(), resp.GetStreams())
			})
		},
	}
	cmd.Flags().
		StringVarP(&output, "output", "o", outputText, "Output: text, or json with the streams as in stream files")
	return cmd
}

func writeStreamTable(out io.Writer, streams []*v1.Stream) error {
	w := tabwriter.NewWriter(out, 0, 0, tablePadding, ' ', 0)
	_, _ = fmt.Fprintln(w, "NAME\tSOURCE\tSTAGES\tSINKS\tSUSPENDED")
	for _, s := range streams {
		_, _ = fmt.Fprintf(w, "%s\t%s\t%d\t%d\t%t\n", s.GetName(), s.GetSource().GetType(),
			len(s.GetStages()), len(s.GetSinks()), s.GetSuspended())
	}
	return w.Flush()
}

func newStreamGetCmd(target *targetFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "get <name>",
		Short: "Write a stream as a stream file",
		Args:  cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withStreams(cmd, target, false, func(ctx context.Context, client v1.StreamServiceClient) error {
				resp, err := client.GetStream(ctx, &v1.GetStreamRequest{Name: args[0]})
				if err != nil {
					return streamError(err)
				}
				return writeIndented(cmd.OutOrStdout(), streamFile(api.StreamFromPB(resp.GetStream())))
			})
		},
	}
}

func newStreamDeleteCmd(target *targetFlags) *cobra.Command {
	return &cobra.Command{
		Use:   "delete <name>...",
		Short: "Delete streams",
		Args:  cobra.MinimumNArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return withStreams(cmd, target, true, func(ctx context.Context, client v1.StreamServiceClient) error {
				var errs []error
				for _, name := range args {
					if _, err := client.DeleteStream(ctx, &v1.DeleteStreamRequest{Name: name}); err != nil {
						errs = append(errs, fmt.Errorf("%s: %w", name, streamError(err)))
						continue
					}
					_, _ = fmt.Fprintf(cmd.OutOrStdout(), "deleted %s\n", name)
				}
				return errors.Join(errs...)
			})
		},
	}
}

type applyOptions struct {
	files  []string
	dryRun bool
}

func newStreamApplyCmd(target *targetFlags) *cobra.Command {
	var opts applyOptions
	cmd := &cobra.Command{
		Use:   "apply -f <file or directory>...",
		Short: "Create or update streams from stream files",
		Long: `Create or update the streams of stream files. Every file is planned first:
an unchanged stream is left alone, and the output says whether the stream
was created, had its stages and sinks replaced below its running source, or
was rebuilt. --dry-run only shows the plan. A directory is read like the
file sync reads it, its *.json files.`,
		Example: `  tram stream apply -f config/
  tram stream apply -f stream.json --dry-run`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return withStreams(
				cmd,
				target,
				!opts.dryRun,
				func(ctx context.Context, client v1.StreamServiceClient) error {
					files, err := streamFiles(opts.files)
					if err != nil {
						return err
					}
					var errs []error
					for _, file := range files {
						line, applyErr := applyFile(ctx, client, file, opts.dryRun)
						if applyErr != nil {
							errs = append(errs, fmt.Errorf("%s: %w", file, applyErr))
							continue
						}
						_, _ = fmt.Fprintln(cmd.OutOrStdout(), line)
					}
					return errors.Join(errs...)
				},
			)
		},
	}
	cmd.Flags().StringSliceVarP(&opts.files, "filename", "f", nil, "Stream files or directories of them")
	cmd.Flags().BoolVar(&opts.dryRun, "dry-run", false, "Only show what applying would do")
	_ = cmd.MarkFlagRequired("filename")
	return cmd
}

// applyFile applies the stream of file, or only plans it, and describes what
// happened.
func applyFile(ctx context.Context, client v1.StreamServiceClient, file string, dryRun bool) (string, error) {
	spec, err := testrun.LoadStream(file)
	if err != nil {
		return "", err
	}
	stream := api.StreamToPB(spec)
	planned, err := client.PutStream(ctx, &v1.PutStreamRequest{Stream: stream, DryRun: true})
	if err != nil {
		return "", streamError(err)
	}
	exists := true
	if _, err = client.GetStream(ctx, &v1.GetStreamRequest{Name: spec.Name}); status.Code(err) == codes.NotFound {
		exists = false
	} else if err != nil {
		return "", streamError(err)
	}
	what := describePlan(planned.GetPlan(), exists)
	if dryRun {
		return fmt.Sprintf("%s: would be %s", spec.Name, what), nil
	}
	if planned.GetPlan().GetAction() == v1.StreamPlanAction_STREAM_PLAN_ACTION_NOOP {
		return spec.Name + ": unchanged", nil
	}
	if _, err = client.PutStream(ctx, &v1.PutStreamRequest{Stream: stream}); err != nil {
		return "", streamError(err)
	}
	return fmt.Sprintf("%s: %s", spec.Name, what), nil
}

func describePlan(plan *v1.StreamPlan, exists bool) string {
	var what string
	switch {
	case !exists:
		what = "created"
	case plan.GetAction() == v1.StreamPlanAction_STREAM_PLAN_ACTION_NOOP:
		return "unchanged"
	case plan.GetAction() == v1.StreamPlanAction_STREAM_PLAN_ACTION_HOT_SWAP:
		what = "updated, stages and sinks replaced"
	case plan.GetSourceReplaced():
		what = "rebuilt with a new source"
	default:
		what = "rebuilt"
	}
	return what
}

func newStreamDiffCmd(target *targetFlags) *cobra.Command {
	var files []string
	cmd := &cobra.Command{
		Use:   "diff -f <file or directory>...",
		Short: "Show how stream files differ from the running streams",
		Long: `Show how the streams of stream files differ from the running ones, as a
unified diff of their stream files, and what applying them would do.`,
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			paths, err := streamFiles(files)
			if err != nil {
				return err
			}
			// diff plans with a dry run, which changes nothing, so it is
			// allowed in Kubernetes too.
			return withStreams(cmd, target, false, func(ctx context.Context, client v1.StreamServiceClient) error {
				var errs []error
				for _, file := range paths {
					if diffErr := diffFile(ctx, cmd.OutOrStdout(), client, file); diffErr != nil {
						errs = append(errs, fmt.Errorf("%s: %w", file, diffErr))
					}
				}
				return errors.Join(errs...)
			})
		},
	}
	cmd.Flags().StringSliceVarP(&files, "filename", "f", nil, "Stream files or directories of them")
	_ = cmd.MarkFlagRequired("filename")
	return cmd
}

func diffFile(ctx context.Context, out io.Writer, client v1.StreamServiceClient, file string) error {
	spec, err := testrun.LoadStream(file)
	if err != nil {
		return err
	}
	running := ""
	exists := true
	resp, err := client.GetStream(ctx, &v1.GetStreamRequest{Name: spec.Name})
	switch {
	case status.Code(err) == codes.NotFound:
		exists = false
	case err != nil:
		return streamError(err)
	default:
		if running, err = indented(streamFile(api.StreamFromPB(resp.GetStream()))); err != nil {
			return err
		}
	}
	// The file goes the same way as the running stream, so only real
	// differences show.
	wanted, err := indented(streamFile(api.StreamFromPB(api.StreamToPB(spec))))
	if err != nil {
		return err
	}
	if exists && running == wanted {
		_, _ = fmt.Fprintf(out, "%s: unchanged\n", spec.Name)
		return nil
	}
	diff, err := difflib.GetUnifiedDiffString(difflib.UnifiedDiff{
		A:        difflib.SplitLines(running),
		B:        difflib.SplitLines(wanted),
		FromFile: "running/" + spec.Name,
		ToFile:   file,
		Context:  diffContext,
	})
	if err != nil {
		return err
	}
	// The dry run builds what applying would build: a stream that cannot
	// be built still shows its diff, with the reason applying would fail.
	planned, err := client.PutStream(ctx, &v1.PutStreamRequest{Stream: api.StreamToPB(spec), DryRun: true})
	if status.Code(err) == codes.InvalidArgument {
		_, _ = fmt.Fprintf(out, "%s: applying would fail:\n%s\n%s",
			spec.Name, indent(status.Convert(err).Message()), diff)
		return errApplyWouldFail
	}
	if err != nil {
		return streamError(err)
	}
	_, _ = fmt.Fprintf(out, "%s: would be %s\n%s", spec.Name, describePlan(planned.GetPlan(), exists), diff)
	return nil
}

// streamFile is spec as in a stream file, with empty specs written as {}.
func streamFile(spec domain.StreamSpec) *configsync.Stream {
	file := configsync.StreamFromDomain(spec)
	file.Stream.Source.Spec = orEmpty(file.Stream.Source.Spec)
	for i := range file.Stream.Stages {
		file.Stream.Stages[i].Spec = orEmpty(file.Stream.Stages[i].Spec)
	}
	for i := range file.Stream.Sinks {
		file.Stream.Sinks[i].Spec = orEmpty(file.Stream.Sinks[i].Spec)
	}
	return file
}

func orEmpty(spec map[string]any) map[string]any {
	if spec == nil {
		return map[string]any{}
	}
	return maps.Clone(spec)
}

func indented(value any) (string, error) {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return "", err
	}
	return string(data) + "\n", nil
}

func writeIndented(out io.Writer, value any) error {
	text, err := indented(value)
	if err != nil {
		return err
	}
	_, err = io.WriteString(out, text)
	return err
}
