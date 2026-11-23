// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package main

import (
	"bufio"
	"bytes"
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/go-tram/tram/internal/testutil/amqptest"
	"github.com/go-tram/tram/internal/testutil/benchcpu"
	"github.com/go-tram/tram/internal/testutil/kafkatest"
	"github.com/go-tram/tram/internal/testutil/natstest"
)

const (
	scenarioPackage  = "./internal/scenario/"
	streamsBenchtime = "1x"

	pageFilePermissions = 0o644
	failureLines        = 30
	maxLineBytes        = 1 << 20
	percent             = 100
	spreadWarning       = 10
)

type options struct {
	out, components, input, raw string
	count                       int
	scenarioTime, componentTime string
}

func main() {
	var opts options
	flag.StringVar(&opts.out, "out", "BENCHMARK.md", "page to update")
	flag.StringVar(&opts.components, "components", "COMPONENTS.md",
		"component documentation whose per-component sections to update")
	flag.StringVar(&opts.input, "input", "", "use this saved benchmark output instead of running the benchmarks")
	flag.StringVar(&opts.raw, "raw", "", "save the raw benchmark output to this file")
	flag.IntVar(&opts.count, "count", 3, "runs per benchmark; the page shows medians")
	flag.StringVar(&opts.scenarioTime, "scenario-benchtime", "1s", "benchtime of the scenario benchmarks")
	flag.StringVar(&opts.componentTime, "component-benchtime", "300ms", "benchtime of the component benchmarks")
	flag.Parse()

	if err := runInterruptible(opts); err != nil {
		log.Print(err)
		os.Exit(1)
	}
}

func runInterruptible(opts options) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()
	return run(ctx, opts)
}

func run(ctx context.Context, opts options) error {
	page, err := os.ReadFile(opts.out)
	if err != nil {
		return err
	}
	env := environment(ctx)

	output, err := benchmarkOutput(ctx, opts, env)
	if err != nil {
		return err
	}

	results, meta := parse(output)
	env.cpu = meta.cpu
	if meta.commit != "" {
		env.commit, env.date = meta.commit, meta.date
	}
	sections := map[string]string{
		"environment":           renderEnvironment(env, opts.count, opts.scenarioTime, opts.componentTime),
		"scenarios":             renderScenarios(results),
		"sizing":                renderSizing(results),
		"streams":               renderStreams(results),
		"streams-fleet":         renderStreamsFleet(results),
		"components-per-record": renderComponents(results, perRecordComponents(), true),
		"components-per-event":  renderComponents(results, perEventComponents(), false),
	}
	if err = updatePage(opts.out, page, sections); err != nil {
		return err
	}
	components, err := os.ReadFile(opts.components)
	if err != nil {
		return err
	}
	return updatePage(opts.components, components, renderComponentSections(results, env))
}

// updatePage replaces the sections of page and writes it to path.
func updatePage(path string, page []byte, sections map[string]string) error {
	var err error
	for name, content := range sections {
		if page, err = replaceSection(page, name, content); err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
	}
	//nolint:gosec // The pages are repository files that everyone may read.
	if err = os.WriteFile(path, page, pageFilePermissions); err != nil {
		return err
	}
	log.Printf("updated %s", path)
	return nil
}

func benchmarkOutput(ctx context.Context, opts options, env env) ([]byte, error) {
	if opts.input != "" {
		return os.ReadFile(opts.input)
	}

	output := fmt.Appendf(nil, "%s%s\n%s%s\n", commitPrefix, env.commit, datePrefix, env.date)
	measured, err := runBenchmarks(ctx, opts.count, opts.scenarioTime, opts.componentTime)
	if err != nil {
		return nil, err
	}
	output = append(output, measured...)
	if opts.raw != "" {
		if err = os.WriteFile(opts.raw, output, pageFilePermissions); err != nil {
			return nil, err
		}
	}
	return output, nil
}

func runBenchmarks(ctx context.Context, count int, scenarioTime, componentTime string) ([]byte, error) {
	var out bytes.Buffer
	log.Print("running scenario benchmarks")
	if err := goTest(ctx, &out, "Scenario", scenarioTime, count, scenarioPackage); err != nil {
		return nil, err
	}
	log.Print("running streams benchmarks")
	if err := goTest(ctx, &out, "Streams", streamsBenchtime, count, scenarioPackage); err != nil {
		return nil, err
	}

	packages, err := goList(ctx, "./internal/...")
	if err != nil {
		return nil, err
	}
	packages = slices.DeleteFunc(packages, func(p string) bool { return strings.HasSuffix(p, "/internal/scenario") })
	log.Print("running component benchmarks")
	if err = goTest(ctx, &out, ".", componentTime, count, packages...); err != nil {
		return nil, err
	}
	return out.Bytes(), nil
}

func goTest(ctx context.Context, out *bytes.Buffer, pattern, benchtime string, count int, packages ...string) error {
	args := append([]string{"test", "-p", "1", "-run", "^$", "-bench", pattern, "-benchmem",
		"-benchtime", benchtime, "-count", strconv.Itoa(count)}, packages...)
	var stdout bytes.Buffer
	cmd := exec.CommandContext(ctx, "go", args...)
	cmd.Stdout = io.MultiWriter(&stdout, progress{})
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		lines := strings.Split(strings.TrimSpace(stdout.String()), "\n")
		return fmt.Errorf("go test: %w\n%s", err, strings.Join(lines[max(0, len(lines)-failureLines):], "\n"))
	}
	out.Write(stdout.Bytes())
	return nil
}

type progress struct{}

func (progress) Write(p []byte) (int, error) {
	for line := range strings.SplitSeq(string(p), "\n") {
		if after, ok := strings.CutPrefix(line, "pkg: "); ok {
			log.Print(after)
		}
	}
	return len(p), nil
}

func goList(ctx context.Context, pattern string) ([]string, error) {
	out, err := exec.CommandContext(ctx, "go", "list", pattern).Output()
	if err != nil {
		return nil, fmt.Errorf("go list: %w", err)
	}
	return strings.Fields(string(out)), nil
}

type env struct {
	commit, date, goVersion, cpu string
}

const (
	commitPrefix = "benchreport-commit: "
	datePrefix   = "benchreport-date: "
)

type metadata struct {
	commit, date, cpu string
}

func environment(ctx context.Context) env {
	commit := strings.TrimSpace(command(ctx, "git", "rev-parse", "--short", "HEAD"))
	if strings.TrimSpace(command(ctx, "git", "status", "--porcelain", "--untracked-files=no")) != "" {
		commit += " (with uncommitted changes)"
	}
	return env{
		commit:    commit,
		date:      time.Now().Format(time.DateOnly),
		goVersion: strings.TrimSpace(command(ctx, "go", "env", "GOVERSION")),
	}
}

func command(ctx context.Context, name string, args ...string) string {
	out, err := exec.CommandContext(ctx, name, args...).Output()
	if err != nil {
		return "unknown"
	}
	return string(out)
}

type result struct {
	metrics map[string]float64
	spread  float64
}

var benchLine = regexp.MustCompile(`^Benchmark(\S+?)(-\d+)?\s+\d+\s+(.*)$`)

func parse(output []byte) (map[string]result, metadata) {
	runs := map[string][]map[string]float64{}
	pkg, meta := "", metadata{}
	scanner := bufio.NewScanner(bytes.NewReader(output))
	scanner.Buffer(make([]byte, maxLineBytes), maxLineBytes)
	for scanner.Scan() {
		line := scanner.Text()
		switch {
		case strings.HasPrefix(line, "pkg: "):
			pkg = strings.TrimPrefix(strings.TrimPrefix(line, "pkg: "), "github.com/go-tram/tram/")
		case strings.HasPrefix(line, "cpu: "):
			meta.cpu = strings.TrimPrefix(line, "cpu: ")
		case strings.HasPrefix(line, commitPrefix):
			meta.commit = strings.TrimPrefix(line, commitPrefix)
		case strings.HasPrefix(line, datePrefix):
			meta.date = strings.TrimPrefix(line, datePrefix)
		default:
			if name, metrics, ok := parseBenchLine(line); ok {
				key := pkg + "|" + name
				runs[key] = append(runs[key], metrics)
			}
		}
	}

	results := map[string]result{}
	for key, rs := range runs {
		results[key] = summarize(rs)
	}
	return results, meta
}

func parseBenchLine(line string) (string, map[string]float64, bool) {
	m := benchLine.FindStringSubmatch(line)
	if m == nil {
		return "", nil, false
	}
	fields := strings.Fields(m[3])
	metrics := map[string]float64{}
	for i := 0; i+1 < len(fields); i += 2 {
		if v, err := strconv.ParseFloat(fields[i], 64); err == nil {
			metrics[fields[i+1]] = v
		}
	}
	_, ok := metrics["ns/op"]
	return m[1], metrics, ok
}

func summarize(runs []map[string]float64) result {
	r := result{metrics: map[string]float64{}}
	for unit := range runs[0] {
		var values []float64
		for _, run := range runs {
			values = append(values, run[unit])
		}
		r.metrics[unit] = median(values)
	}
	var ns []float64
	for _, run := range runs {
		ns = append(ns, run["ns/op"])
	}
	r.spread = (slices.Max(ns) - slices.Min(ns)) / median(ns) * percent
	return r
}

func median(values []float64) float64 {
	sorted := slices.Clone(values)
	sort.Float64s(sorted)
	middle := len(sorted) / 2 //nolint:mnd // Halving finds the middle.
	if len(sorted)%2 == 1 {
		return sorted[middle]
	}
	return (sorted[middle-1] + sorted[middle]) / 2 //nolint:mnd // The mean of the two middle values.
}

var recordsInName = regexp.MustCompile(`records=(\d+)`)

func recordsPerOp(name string) float64 {
	m := recordsInName.FindStringSubmatch(name)
	if m == nil {
		return 1
	}
	n, _ := strconv.ParseFloat(m[1], 64)
	return n
}

func renderEnvironment(e env, count int, scenarioTime, componentTime string) string {
	var b strings.Builder
	b.WriteString("| | |\n|---|---|\n")
	fmt.Fprintf(&b, "| Commit | `%s` |\n", e.commit)
	fmt.Fprintf(&b, "| Date | %s |\n", e.date)
	fmt.Fprintf(&b, "| CPU | %s |\n", or(e.cpu, "unknown"))
	fmt.Fprintf(&b, "| Go | %s |\n", e.goVersion)
	fmt.Fprintf(&b, "| NATS | `%s` in a local container |\n", natstest.Image)
	fmt.Fprintf(&b, "| Kafka | `%s` in a local container |\n", kafkatest.Image)
	fmt.Fprintf(&b, "| AMQP | `%s` in a local container |\n", amqptest.Image)
	fmt.Fprintf(
		&b,
		"| Runs | %d per benchmark, medians shown; scenarios `-benchtime=%s`, streams `-benchtime=%s`, components `-benchtime=%s` |\n",
		count,
		scenarioTime,
		streamsBenchtime,
		componentTime,
	)
	return b.String()
}

func scenarioOrder() []struct{ name, title string } {
	return []struct{ name, title string }{
		{"otlp-nats", "OTLP → NATS"},
		{"otlp-batch-nats", "OTLP → batch → NATS"},
		{"otlp-split-nats", "OTLP → 2 × filter → 2 × NATS"},
		{scenarioNATSFilterNATS, "NATS → filter → NATS"},
	}
}

const scenarioPkg = "internal/scenario"

func renderScenarios(results map[string]result) string {
	var b strings.Builder
	for _, sc := range scenarioOrder() {
		var keys []string
		for key := range results {
			if strings.HasPrefix(key, scenarioPkg+"|Scenario/"+sc.name+"/") {
				keys = append(keys, key)
			}
		}
		if len(keys) == 0 {
			continue
		}
		sort.Slice(keys, func(i, j int) bool { return scenarioSortKey(keys[i]) < scenarioSortKey(keys[j]) })

		fmt.Fprintf(&b, "#### `%s`: %s\n\n", sc.name, sc.title)
		b.WriteString(
			"| Callers | Records per event | Records/s | Events/s | CPU per record | Records/s per core | p50 latency | p99 latency | Allocs per record | Spread |\n",
		)
		b.WriteString("|---|---|---|---|---|---|---|---|---|---|\n")
		for _, key := range keys {
			r := results[key]
			name := strings.TrimPrefix(key, scenarioPkg+"|")
			records := recordsPerOp(name)
			fmt.Fprintf(&b, "| %s | %s | %s | %s | %s | %s | %s | %s | %.0f | %s |\n",
				param(name, "concurrency"), resourcesLabel(name),
				count(r.metrics["records/s"]), count(r.metrics["events/s"]),
				duration(r.metrics["cpu-ns/record"]), count(r.metrics["records/s/core"]),
				millis(r.metrics, "p50-ms"), millis(r.metrics, "p99-ms"),
				r.metrics[allocsPerOp]/records, spread(r.spread))
		}
		b.WriteString("\n")
	}
	if b.Len() == 0 {
		return "_No scenario results; the scenario benchmarks need Docker or Podman._\n"
	}
	return b.String()
}

func scenarioSortKey(key string) string {
	return fmt.Sprintf("%03s/%06s", param(key, "concurrency"), param(key, "records"))
}

func param(name, key string) string {
	m := regexp.MustCompile(key + `=(\d+)`).FindStringSubmatch(name)
	if m == nil {
		return ""
	}
	return m[1]
}

func resourcesLabel(name string) string {
	return fmt.Sprintf(
		"%s (%s %s)",
		param(name, "records"),
		param(name, "resources"),
		plural(param(name, "resources"), "service", "services"),
	)
}

func plural(n, one, many string) string {
	if n == "1" {
		return one
	}
	return many
}

func sizingRows() []struct{ label, name string } {
	return []struct{ label, name string }{
		{
			"Batched: OTLP → batch → NATS, 1,000 records per request, 8 callers",
			"Scenario/otlp-batch-nats/concurrency=8/records=1000/resources=10/teams=1",
		},
		{
			"Split: OTLP → 2 × filter → 2 × NATS, 1,000 records per request, 8 callers",
			"Scenario/otlp-split-nats/concurrency=8/records=1000/resources=10/teams=2",
		},
		{
			"Forwarding: NATS → filter → NATS, 1,000 records per message, 8 workers",
			"Scenario/nats-filter-nats/concurrency=8/records=1000/resources=10/teams=1",
		},
		{
			"Batched: OTLP → batch → Kafka, 1,000 records per request, 8 callers",
			"Scenario/otlp-batch-kafka/concurrency=8/records=1000/resources=10/teams=1",
		},
		{
			"Batched: OTLP → batch → AMQP, 1,000 records per request, 8 callers",
			"Scenario/otlp-batch-amqp/concurrency=8/records=1000/resources=10/teams=1",
		},
		{
			"Unbatched: OTLP → NATS, 1 record per request, 8 callers",
			"Scenario/otlp-nats/concurrency=8/records=1/resources=1/teams=1",
		},
	}
}

func renderSizing(results map[string]result) string {
	var b strings.Builder
	b.WriteString(
		"| Pipeline | CPU per record | Cores for 100 k records/s | Cores for 1 M records/s |\n|---|---|---|---|\n",
	)
	for _, row := range sizingRows() {
		r, ok := results[scenarioPkg+"|"+row.name]
		if !ok {
			fmt.Fprintf(&b, "| %s | n/a | n/a | n/a |\n", row.label)
			continue
		}
		cpu := r.metrics["cpu-ns/record"]
		fmt.Fprintf(&b, "| %s | %s | %.1f | %.1f |\n",
			row.label, duration(cpu), cpu*hundredThousand/billion, cpu*million/billion)
	}
	return b.String()
}

const scenarioNATSFilterNATS = "nats-filter-nats"

func streamCounts() []int {
	return []int{10, 100, 1000}
}

const fleetBaseStreams = 1000

const minStreamsForIdleCPU = 1000

func streamsShapes() []struct{ name, workers, title string } {
	return []struct{ name, workers, title string }{
		{scenarioNATSFilterNATS, "1", "NATS (1 worker) → filter → NATS"},
		{scenarioNATSFilterNATS, "4", "NATS (4 workers) → filter → NATS"},
		{"otlp-batch-nats", "0", "OTLP → batch → NATS"},
	}
}

func streamsKey(shape, workers string, streams int) string {
	return fmt.Sprintf("%s|Streams/%s/workers=%s/streams=%d", scenarioPkg, shape, workers, streams)
}

func renderStreams(results map[string]result) string {
	var b strings.Builder
	b.WriteString(
		"| Stream | Streams | Build | Heap | Goroutines | NATS connections | NATS messages/s, idle | Idle CPU of a core |\n",
	)
	b.WriteString("|---|---|---|---|---|---|---|---|\n")
	rows := 0
	for _, shape := range streamsShapes() {
		for _, n := range streamCounts() {
			r, ok := results[streamsKey(shape.name, shape.workers, n)]
			if !ok {
				continue
			}
			rows++
			m := r.metrics
			idleCPU := "–"
			if n >= minStreamsForIdleCPU {
				idleCPU = coreShare(m["idle-cpu-µs/s/stream"])
			}
			fmt.Fprintf(&b, "| %s | %s | %s | %.0f KiB | %.1f | %.1f | %.2f | %s |\n",
				shape.title, streamCount(n),
				duration(m["build-µs/stream"]*thousand), m["heap-KiB/stream"], m["goroutines/stream"],
				m["nats-conns/stream"], m["nats-msgs/s/stream"], idleCPU)
		}
	}
	if rows == 0 {
		return "_No streams results; the streams benchmarks need Docker or Podman._\n"
	}
	return b.String()
}

func fleetSizes() []struct{ streams, replicas int } {
	return []struct{ streams, replicas int }{
		{100, 1}, {100, 10}, {1000, 1}, {1000, 3}, {1000, 10},
	}
}

func renderStreamsFleet(results map[string]result) string {
	var b strings.Builder
	b.WriteString(
		"| Stream | Streams | Replicas | Heap per replica | Idle CPU per replica, of a core | NATS connections | NATS messages/s, idle |\n",
	)
	b.WriteString("|---|---|---|---|---|---|---|\n")
	rows := 0
	for _, shape := range streamsShapes() {
		r, ok := results[streamsKey(shape.name, shape.workers, fleetBaseStreams)]
		if !ok {
			continue
		}
		m := r.metrics
		for _, size := range fleetSizes() {
			rows++
			n, replicas := float64(size.streams), float64(size.replicas)
			fmt.Fprintf(&b, "| %s | %s | %d | %.0f MiB | %s | %s | %s |\n",
				shape.title, streamCount(size.streams), size.replicas,
				m["heap-KiB/stream"]*n/kibibyte, coreShare(m["idle-cpu-µs/s/stream"]*n),
				count(m["nats-conns/stream"]*n*replicas), count(m["nats-msgs/s/stream"]*n*replicas))
		}
	}
	if rows == 0 {
		return "_No streams results; the streams benchmarks need Docker or Podman._\n"
	}
	return b.String()
}

func coreShare(microsPerSecond float64) string {
	return fmt.Sprintf("%.2g %%", microsPerSecond/million*percent)
}

func streamCount(n int) string {
	if n < thousand {
		return strconv.Itoa(n)
	}
	return fmt.Sprintf("%d,%03d", n/thousand, n%thousand)
}

// component is a row of the component tables. section names the section
// of the component documentation the row also goes to, empty for rows that
// measure no single component, such as serializing OTLP data.
type component struct{ label, pkg, name, section string }

const (
	pkgCore         = "internal/core"
	pkgDomain       = "internal/core/domain"
	pkgBrokerSink   = "internal/adapters/sink/broker"
	pkgBrokerSource = "internal/adapters/source/broker"
	pkgOTLPSource   = "internal/adapters/source/otlp"
	pkgSourceBase   = "internal/adapters/source/base"
	pkgBatch        = "internal/adapters/stage/batch"
	pkgCELFilter    = "internal/adapters/stage/filter/cel"
	pkgCELValidator = "internal/adapters/stage/validation/cel"
	pkgValueMap     = "internal/adapters/stage/valuemap"
	pkgTransform    = "internal/adapters/stage/transform"
	pkgEncryption   = "internal/adapters/stage/encryption"
	pkgDecryption   = "internal/adapters/stage/decryption"
	pkgBufferSink   = "internal/adapters/sink/buffer"
	pkgBufferSource = "internal/adapters/source/buffer"
	pkgOTLPSink     = "internal/adapters/sink/otlp"
)

// The sections of the component documentation, one per component that has
// benchmarks, in the order of the documentation.
const (
	sectionOTLPSource   = "otlp-source"
	sectionBrokerSource = "broker-source"
	sectionFilter       = "filter"
	sectionValidation   = "validation"
	sectionBatch        = "batch"
	sectionEncryption   = "encryption"
	sectionDecryption   = "decryption"
	sectionNoop         = "noop"
	sectionValueMap     = "value-map"
	sectionTransform    = "transform"
	sectionBrokerSink   = "broker-sink"
	sectionBufferSource = "buffer-source"
	sectionBufferSink   = "buffer-sink"
	sectionOTLPSink     = "otlp-sink"
)

func componentSections() []string {
	return []string{
		sectionOTLPSource, sectionBrokerSource, sectionBufferSource,
		sectionFilter, sectionValidation, sectionBatch, sectionEncryption, sectionDecryption,
		sectionNoop, sectionValueMap, sectionTransform,
		sectionOTLPSink, sectionBrokerSink, sectionBufferSink,
	}
}

func perRecordComponents() []component {
	rows := []component{
		{
			"Broker sink (NATS): group and publish, 10 subjects",
			pkgBrokerSink,
			"Notify/records=1000/resources=10/teams=10", sectionBrokerSink,
		},
		{
			"Broker sink (NATS): group and publish, 1 subject",
			pkgBrokerSink,
			"Notify/records=1000/resources=10/teams=1", sectionBrokerSink,
		},
		{
			"Broker sink (NATS): group and publish, 100 subjects",
			pkgBrokerSink,
			"Notify/records=1000/resources=10/teams=100", sectionBrokerSink,
		},
		{
			"Broker sink (NATS): group and publish, 1,000 subjects (a message per record)",
			pkgBrokerSink,
			"Notify/records=1000/resources=10/teams=1000", sectionBrokerSink,
		},
		{
			"Serializing OTLP data and back (logs)",
			pkgDomain,
			"OTLPDataRoundTrip/logs/records=1000/resources=10/teams=1",
			"",
		},
		{
			"Broker source (NATS), 1 worker",
			pkgBrokerSource,
			"Consume/workers=1/records=1000/resources=10/teams=1",
			sectionBrokerSource,
		},
		{
			"Broker source (NATS), 8 workers",
			pkgBrokerSource,
			"Consume/workers=8/records=1000/resources=10/teams=1",
			sectionBrokerSource,
		},
		{
			"OTLP source: decoding a request",
			pkgOTLPSource,
			"HandleLogs/records=1000/resources=10/teams=1", sectionOTLPSource,
		},
		{
			"filter (CEL), keeping half of the records",
			pkgCELFilter,
			"CELFilter/keep-half/records=1000/resources=10/teams=2", sectionFilter,
		},
		{
			"filter (CEL), keeping all records",
			pkgCELFilter,
			"CELFilter/keep-all/records=1000/resources=10/teams=2", sectionFilter,
		},
		{
			"validation (CEL), record-level expression",
			pkgCELValidator,
			"CELValidator/record-attribute/records=1000/resources=10/teams=1", sectionValidation,
		},
		{
			"validation (CEL), resource-level expression",
			pkgCELValidator,
			"CELValidator/resource-attribute/records=1000/resources=10/teams=1", sectionValidation,
		},
		{
			"Record filter helper without CEL",
			pkgDomain,
			"Filter/logs/keep-half/records=1000/resources=10/teams=2",
			"",
		},
		{
			"batch, 8 concurrent callers",
			pkgBatch,
			"Batch/parallel-8-events/records=1000/resources=10/teams=1", sectionBatch,
		},
	}
	return append(append(rows, perRecordDrivers()...), perRecordStages()...)
}

// perRecordDrivers are the per-record rows of the other drivers and
// protocols of sources and sinks, next to the NATS and OTLP/HTTP rows.
func perRecordDrivers() []component {
	return []component{
		{"OTLP source over gRPC: request with the client's encoding", pkgOTLPSource,
			"GRPCExportLogs/records=1000/resources=10/teams=1", sectionOTLPSource},
		{"Broker source (Kafka), 8 workers", pkgBrokerSource,
			"ConsumeKafka/workers=8/records=1000/resources=10/teams=1", sectionBrokerSource},
		{"Broker source (AMQP), 8 workers", pkgBrokerSource,
			"ConsumeAMQP/workers=8/records=1000/resources=10/teams=1", sectionBrokerSource},
		{"Buffer source (SQLite), 1 worker", pkgBufferSource,
			"Consume/workers=1/records=1000/resources=10/teams=1", sectionBufferSource},
		{"Buffer source (SQLite), 8 workers", pkgBufferSource,
			"Consume/workers=8/records=1000/resources=10/teams=1", sectionBufferSource},
		{"OTLP sink: encode and send over loopback", pkgOTLPSink,
			"Notify/records=1000/resources=10/teams=1", sectionOTLPSink},
		{"Broker sink (Kafka): group and publish, 10 topics", pkgBrokerSink,
			"NotifyKafka/records=1000/resources=10/teams=10", sectionBrokerSink},
		{"Broker sink (AMQP): group and publish, 10 queues", pkgBrokerSink,
			"NotifyAMQP/records=1000/resources=10/teams=10", sectionBrokerSink},
		{"Buffer sink (SQLite), one caller", pkgBufferSink,
			"Notify/serial/records=1000/resources=10/teams=1", sectionBufferSink},
		{"Buffer sink (SQLite), concurrent callers", pkgBufferSink,
			"Notify/parallel/records=1000/resources=10/teams=1", sectionBufferSink},
	}
}

// perRecordStages are the per-record rows of the stages that transform or
// annotate records, and of the noop stage as the baseline.
func perRecordStages() []component {
	return []component{
		{
			"value_map, mapping a record attribute",
			pkgValueMap,
			"NotifyLogs/hit/records=1000/resources=10/teams=2",
			sectionValueMap,
		},
		{
			"value_map, mapping a resource attribute",
			pkgValueMap,
			"NotifyLogs/resource-level/records=1000/resources=10/teams=2",
			sectionValueMap,
		},
		{
			"transform (CEL), one entry statement",
			pkgTransform,
			"NotifyLogs/set/records=1000/resources=10/teams=2",
			sectionTransform,
		},
		{
			"transform (CEL), four entry statements of a normalization",
			pkgTransform,
			"NotifyLogs/normalize/records=1000/resources=10/teams=2",
			sectionTransform,
		},
		{
			"transform (CEL), one resource statement",
			pkgTransform,
			"NotifyLogs/resource/records=1000/resources=10/teams=2",
			sectionTransform,
		},
		{
			"encryption, body only",
			pkgEncryption,
			"NotifyLogs/body-only/records=1000/resources=10/teams=2",
			sectionEncryption,
		},
		{
			"encryption, body and attributes",
			pkgEncryption,
			"NotifyLogs/body-plus-attributes/records=1000/resources=10/teams=2",
			sectionEncryption,
		},
		{
			"decryption, body only",
			pkgDecryption,
			"NotifyLogs/body-only/records=1000/resources=10/teams=2",
			sectionDecryption,
		},
		{
			"decryption, body and attributes",
			pkgDecryption,
			"NotifyLogs/body-plus-attributes/records=1000/resources=10/teams=2",
			sectionDecryption,
		},
		{"noop stage", pkgCore, "Pipeline/noop/records=1000/resources=10/teams=1", sectionNoop},
	}
}

func perEventComponents() []component {
	return []component{
		{
			"Broker sink (NATS): one record (waits for the acknowledgement)",
			pkgBrokerSink,
			"Notify/records=1/resources=1/teams=1", sectionBrokerSink,
		},
		{
			"OTLP source: request with one record",
			pkgOTLPSource,
			"HandleLogs/records=1/resources=1/teams=1", sectionOTLPSource,
		},
		{"OTLP source over gRPC: request with one record", pkgOTLPSource,
			"GRPCExportLogs/records=1/resources=1/teams=1", sectionOTLPSource},
		{"Broker sink (Kafka): one record (waits for the acknowledgement)", pkgBrokerSink,
			"NotifyKafka/records=1/resources=1/teams=1", sectionBrokerSink},
		{"Broker sink (AMQP): one record (waits for the acknowledgement)", pkgBrokerSink,
			"NotifyAMQP/records=1/resources=1/teams=1", sectionBrokerSink},
		{"OTLP sink: one record (waits for the answer)", pkgOTLPSink,
			"Notify/records=1/resources=1/teams=1", sectionOTLPSink},
		{"Buffer sink (SQLite): one record, one caller (waits for the sync)", pkgBufferSink,
			"Notify/serial/records=1/resources=1/teams=1", sectionBufferSink},
		{"Buffer sink (SQLite): one record, concurrent callers", pkgBufferSink,
			"Notify/parallel/records=1/resources=1/teams=1", sectionBufferSink},
		{"Buffer source (SQLite): one record, 1 worker", pkgBufferSource,
			"Consume/workers=1/records=1/resources=1/teams=1", sectionBufferSource},
		{"batch, one caller", pkgBatch, "Batch/flush-per-event/records=1/resources=1/teams=1", sectionBatch},
		{"Passing an event to 1 child", pkgSourceBase, "NotifySubscribers/children=1", ""},
		{"Passing an event to 8 children", pkgSourceBase, "NotifySubscribers/children=8", ""},
		{"Each additional noop stage", pkgCore, "Pipeline/noop/records=1/resources=1/teams=1", sectionNoop},
	}
}

func renderComponents(results map[string]result, components []component, perRecord bool) string {
	var b strings.Builder
	if perRecord {
		b.WriteString("| Component | Per record | CPU per record | Allocs per record | Records/s | Spread |\n" +
			"|---|---|---|---|---|---|\n")
	} else {
		b.WriteString("| Component | Per event | CPU per event | Allocs per event | Spread |\n|---|---|---|---|---|\n")
	}
	for _, c := range components {
		r, ok := results[c.pkg+"|"+c.name]
		if !ok {
			if perRecord {
				fmt.Fprintf(&b, "| %s | n/a | n/a | n/a | n/a | |\n", c.label)
			} else {
				fmt.Fprintf(&b, "| %s | n/a | n/a | n/a | |\n", c.label)
			}
			continue
		}
		cost := componentCost(results, c, r)
		if perRecord {
			records := recordsPerOp(c.name)
			fmt.Fprintf(
				&b,
				"| %s | %s | %s | %.1f | %s | %s |\n",
				c.label,
				duration(cost.ns/records),
				cpuDuration(cost.cpu, records),
				cost.allocs/records,
				count(records*billion/cost.ns),
				spread(r.spread),
			)
		} else {
			fmt.Fprintf(&b, "| %s | %s | %s | %.0f | %s |\n",
				c.label, duration(cost.ns), cpuDuration(cost.cpu, 1), cost.allocs, spread(r.spread))
		}
	}
	return b.String()
}

// renderComponentSections renders, per section of the component
// documentation, the rows of that component with a pointer to the
// environment and to the comparison with other components.
func renderComponentSections(results map[string]result, e env) map[string]string {
	sections := map[string]string{}
	for _, section := range componentSections() {
		var b strings.Builder
		if rows := inSection(perRecordComponents(), section); len(rows) > 0 {
			b.WriteString(renderComponents(results, rows, true))
		}
		if rows := inSection(perEventComponents(), section); len(rows) > 0 {
			if b.Len() > 0 {
				b.WriteString("\n")
			}
			b.WriteString(renderComponents(results, rows, false))
		}
		fmt.Fprintf(&b, "\nMeasured on %s, commit `%s`, %s; compare with other components under "+
			"[Where the Time Goes](BENCHMARK.md#where-the-time-goes).\n", or(e.cpu, "unknown CPU"), e.commit, e.date)
		sections["component:"+section] = b.String()
	}
	return sections
}

func inSection(components []component, section string) []component {
	return slices.DeleteFunc(slices.Clone(components), func(c component) bool { return c.section != section })
}

// cost is what a component takes per operation: wall time, CPU time of the
// process (0 when the benchmark does not measure it) and allocations.
type cost struct{ ns, cpu, allocs float64 }

// componentCost returns the costs of a component. The noop stage is measured
// as a pipeline, so the costs of the direct pipeline are subtracted from it.
func componentCost(results map[string]result, c component, r result) cost {
	k := cost{ns: r.metrics["ns/op"], cpu: r.metrics[benchcpu.Unit], allocs: r.metrics[allocsPerOp]}
	if c.name != "Pipeline/noop/records=1/resources=1/teams=1" {
		return k
	}
	if direct, found := results[c.pkg+"|Pipeline/direct/records=1/resources=1/teams=1"]; found {
		k.ns -= direct.metrics["ns/op"]
		k.cpu -= direct.metrics[benchcpu.Unit]
		k.allocs -= direct.metrics[allocsPerOp]
	}
	return k
}

// cpuDuration formats the CPU time per operation divided by n, or n/a when
// the benchmark does not measure it.
func cpuDuration(cpu, n float64) string {
	if cpu <= 0 {
		return "n/a"
	}
	return duration(cpu / n)
}

func replaceSection(page []byte, name, content string) ([]byte, error) {
	begin := fmt.Sprintf("<!-- BEGIN benchreport:%s -->", name)
	end := fmt.Sprintf("<!-- END benchreport:%s -->", name)
	s := string(page)
	i, j := strings.Index(s, begin), strings.Index(s, end)
	if i < 0 || j < i {
		return nil, fmt.Errorf("section %q not found; add %s and %s to the page", name, begin, end)
	}
	return []byte(s[:i+len(begin)] + "\n" + content + s[j:]), nil
}

const (
	allocsPerOp = "allocs/op"

	kibibyte        = 1024
	thousand        = 1e3
	tenThousand     = 1e4
	hundredThousand = 1e5
	million         = 1e6
	billion         = 1e9
)

func duration(ns float64) string {
	switch {
	case ns >= million:
		return fmt.Sprintf("%.1f ms", ns/million)
	case ns >= thousand:
		return fmt.Sprintf("%.2f µs", ns/thousand)
	default:
		return fmt.Sprintf("%.1f ns", ns)
	}
}

func count(x float64) string {
	switch {
	case x >= billion:
		return fmt.Sprintf("%.2f G", x/billion)
	case x >= million:
		return fmt.Sprintf("%.2f M", x/million)
	case x >= tenThousand:
		return fmt.Sprintf("%.0f k", x/thousand)
	case x >= thousand:
		return fmt.Sprintf("%.1f k", x/thousand)
	default:
		return fmt.Sprintf("%.0f", x)
	}
}

func millis(metrics map[string]float64, unit string) string {
	v, ok := metrics[unit]
	if !ok {
		return "–"
	}
	return fmt.Sprintf("%.2f ms", v)
}

func spread(percentage float64) string {
	if percentage > spreadWarning {
		return fmt.Sprintf("±%.0f%% ⚠️", percentage)
	}
	return fmt.Sprintf("±%.0f%%", percentage)
}

func or(s, fallback string) string {
	if s == "" {
		return fallback
	}
	return s
}
