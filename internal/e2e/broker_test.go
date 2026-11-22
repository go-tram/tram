// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package e2e_test

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/broker/partition"
	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/testutil/otlpgen"
)

// The broker sink and source with the NATS driver. TRAM creates no streams,
// so every test sets its streams up first, as a deployment does outside TRAM.
func TestBrokerNATS(t *testing.T) {
	t.Parallel()

	n := startNATS(t)

	t.Run("batch then publish one message per service", func(t *testing.T) {
		t.Parallel()

		out := n.brokerStream(t, "out")
		p := newPipeline(t,
			[]domain.StageSpec{batchStage(6, "5s")},
			[]domain.SinkSpec{n.brokerSinkByService("out", "batch", out)})

		codes := p.postConcurrently(t,
			request("checkout", rec{"c1", "A"}, rec{"c2", "B"}),
			request("cart", rec{"k1", "A"}, rec{"k2", "B"}),
			request("search", rec{"s1", "A"}, rec{"s2", "B"}),
		)
		assert.Equal(t, []int{200, 200, 200}, codes, "requests are answered once the batch is in JetStream")

		messages := n.read(t, out)
		require.Len(t, messages, 3, "one message per service")
		for _, m := range messages {
			assert.Empty(t, m.Header, "messages carry plain OTLP and no headers")
		}
		assert.Equal(t, map[string][]string{
			out + ".checkout": {"checkout/e2e/c1", "checkout/e2e/c2"},
			out + ".cart":     {"cart/e2e/k1", "cart/e2e/k2"},
			out + ".search":   {"search/e2e/s1", "search/e2e/s2"},
		}, logsBySubject(t, messages))
	})

	t.Run("route each request by the router its header names", func(t *testing.T) {
		t.Parallel()

		out := n.brokerStream(t, "routers")
		p := newEmptyPipeline(t, domain.SourceSpec{})
		port := onFreePort(t, 0, func(port int) error {
			p.in = domain.SourceSpec{Name: "in", Type: "otlp", Spec: map[string]any{
				"http_port":         float64(port),
				"header_attributes": map[string]any{"X-Tram-Router": "tram.router.id"},
			}}
			return p.tryUpdate([]domain.StageSpec{batchStage(3, "5s")}, []domain.SinkSpec{{
				Name: "routers", Type: "broker", From: "batch", Spec: n.brokerSinkSpec(map[string]any{
					"logs": fmt.Sprintf("'%s.' + resource.attributes.get_attr('tram.router.id') + '.logs'", out),
				}),
			}})
		})
		p.baseURL = fmt.Sprintf("http://127.0.0.1:%d", port)
		// The third request claims router b in its payload, but its header,
		// which the ingress sets, says a.
		spoofed := request("search", rec{"s1", "A"})
		spoofed.ResourceLogs[0].Resource.Attributes = append(spoofed.ResourceLogs[0].Resource.Attributes,
			stringAttr("tram.router.id", "b"))

		var codes [3]int
		var wg sync.WaitGroup
		wg.Go(func() { codes[0] = p.postWithHeader(t, request("checkout", rec{"c1", "A"}), "X-Tram-Router", "a") })
		wg.Go(func() { codes[1] = p.postWithHeader(t, request("cart", rec{"k1", "B"}), "X-Tram-Router", "b") })
		wg.Go(func() { codes[2] = p.postWithHeader(t, spoofed, "X-Tram-Router", "a") })
		wg.Wait()
		assert.Equal(t, [3]int{200, 200, 200}, codes)

		assert.Equal(t, map[string][]string{
			out + ".a.logs": {"checkout/e2e/c1", "search/e2e/s1"},
			out + ".b.logs": {"cart/e2e/k1"},
		}, logsBySubject(t, n.read(t, out)), "one batch, split by the router of each request's header")
	})

	t.Run("split into two streams", func(t *testing.T) {
		t.Parallel()

		outA, outRest := n.brokerStream(t, "a"), n.brokerStream(t, "rest")
		p := newPipeline(t,
			[]domain.StageSpec{
				customerFilter(customerA, "in", isCustomerA),
				customerFilter(otherCustomers, "in", "!"+isCustomerA),
			},
			[]domain.SinkSpec{
				n.brokerSinkByService(sinkA, customerA, outA),
				n.brokerSinkByService(sinkRest, otherCustomers, outRest),
			})

		require.Equal(t, http.StatusOK,
			p.post(t, request("shop", rec{"r1", "A"}, rec{"r2", "B"}, rec{"r3", "A"}, rec{"r4", "C"}), 10*time.Second))
		assert.Equal(t, map[string][]string{outA + ".shop": {"shop/e2e/r1", "shop/e2e/r3"}},
			logsBySubject(t, n.read(t, outA)))
		assert.Equal(t, map[string][]string{outRest + ".shop": {"shop/e2e/r2", "shop/e2e/r4"}},
			logsBySubject(t, n.read(t, outRest)))
	})

	t.Run("sink to source round trip", func(t *testing.T) {
		t.Parallel()

		mid := n.brokerStream(t, "mid")
		ingest := newPipeline(t, nil, []domain.SinkSpec{n.brokerSinkByService("out", "in", mid)})
		forward := newStream(t, n.brokerSource(mid, "logs", mid+".>"),
			[]domain.StageSpec{customerFilter(customerA, "in", isCustomerA)},
			[]domain.SinkSpec{captureSinkSpec("received", customerA)})

		require.Equal(t, http.StatusOK,
			ingest.post(t, request("shop", rec{"r1", "A"}, rec{"r2", "B"}, rec{"r3", "A"}), 10*time.Second))
		require.Equal(t, http.StatusOK, ingest.post(t, request("cart", rec{"k1", "B"}, rec{"k2", "A"}), 10*time.Second))

		received := forward.sinks.sink(t, "received")
		require.Eventually(t, func() bool { return len(arrived(received.received())) == 3 },
			10*time.Second, 10*time.Millisecond)
		assert.ElementsMatch(t, []string{"shop/e2e/r1", "shop/e2e/r3", "cart/e2e/k2"}, arrived(received.received()))
	})

	t.Run("one source per signal", func(t *testing.T) {
		t.Parallel()
		testOneSourcePerSignal(t, n)
	})

	t.Run("plain OTLP from another producer is read", func(t *testing.T) {
		t.Parallel()

		mid := n.brokerStream(t, "foreign")
		body, err := proto.Marshal(request("foreign", rec{"f1", "A"}, rec{"f2", "B"}))
		require.NoError(t, err)
		_, err = n.js.PublishMsg(t.Context(), &nats.Msg{Subject: mid + ".logs", Data: body})
		require.NoError(t, err)

		forward := newStream(t, n.brokerSource(mid, "logs", mid+".>"), nil,
			[]domain.SinkSpec{captureSinkSpec("received", "in")})
		received := forward.sinks.sink(t, "received")
		require.Eventually(t, func() bool { return len(arrived(received.received())) == 2 },
			10*time.Second, 10*time.Millisecond)
		assert.ElementsMatch(t, []string{"foreign/e2e/f1", "foreign/e2e/f2"}, arrived(received.received()))
	})

	t.Run("partitioned by customer", func(t *testing.T) {
		t.Parallel()
		testPartitionedByCustomer(t, n)
	})

	t.Run("routing by record attribute", func(t *testing.T) {
		t.Parallel()
		testRoutingByRecordAttribute(t, n)
	})

	t.Run("workers fill a batch", func(t *testing.T) {
		t.Parallel()

		const messages = 300
		mid := n.brokerStream(t, "workers")
		publishLogs(t, n, mid+".logs", messages)

		in := n.brokerSource(mid, "logs", mid+".>")
		in.Spec["workers"] = float64(messages)
		forward := newStream(t, in, []domain.StageSpec{batchStage(messages, "30s")},
			[]domain.SinkSpec{captureSinkSpec("received", "batch")})

		received := forward.sinks.sink(t, "received")
		require.Eventually(t, func() bool { return len(received.received()) > 0 }, 10*time.Second, 10*time.Millisecond,
			"the batch was not filled by the messages in flight")
		events := received.received()
		require.Len(t, events, 1)
		assert.Equal(t, messages, events[0].RecordCount())

		cons, err := n.js.Consumer(t.Context(), mid, mid)
		require.NoError(t, err)
		require.Eventually(t, func() bool {
			info, infoErr := cons.Info(t.Context())
			return infoErr == nil && info.NumAckPending == 0 && info.NumPending == 0
		}, 10*time.Second, 10*time.Millisecond, "the messages were not acknowledged")
	})

	t.Run("at most workers messages are buffered", func(t *testing.T) {
		t.Parallel()
		testBrokerBuffer(t, n)
	})

	t.Run("stopping an idle source is fast", func(t *testing.T) {
		t.Parallel()

		mid := n.brokerStream(t, "idle")
		forward := newStream(t, n.brokerSource(mid, "logs", mid+".>"), nil,
			[]domain.SinkSpec{captureSinkSpec("received", "in")})
		time.Sleep(500 * time.Millisecond)

		start := time.Now()
		require.NoError(t, forward.core.DeleteStream(context.Background(), "e2e"))
		assert.Less(t, time.Since(start), time.Second)
	})

	t.Run("a destination without a stream reaches the client", func(t *testing.T) {
		t.Parallel()

		p := newPipeline(t, nil, []domain.SinkSpec{{
			Name: "out", Type: "broker", From: "in",
			Spec: n.brokerSinkSpec(map[string]any{"logs": "'nowhere.logs'"}),
		}})
		assert.Equal(t, http.StatusServiceUnavailable, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
	})

	t.Run("an invalid destination rejects the event", func(t *testing.T) {
		t.Parallel()

		mid := n.brokerStream(t, "invalid")
		p := newPipeline(t, nil, []domain.SinkSpec{{
			Name: "out", Type: "broker", From: "in",
			Spec: n.brokerSinkSpec(map[string]any{"logs": fmt.Sprintf("'%s.' + 'a b'", mid)}),
		}})
		assert.Equal(t, http.StatusUnprocessableEntity, p.post(t, request("shop", rec{"r1", "A"}), 10*time.Second))
		assert.Empty(t, n.read(t, mid))
	})

	t.Run("a message above max_payload rejects the event", func(t *testing.T) {
		t.Parallel()

		mid := n.brokerStream(t, "large")
		p := newPipeline(t, nil, []domain.SinkSpec{n.brokerSinkByService("out", "in", mid)})
		// The default max_payload of the NATS server is 1 MB.
		large := request("shop", rec{strings.Repeat("x", 2<<20), "A"})
		assert.Equal(t, http.StatusUnprocessableEntity, p.post(t, large, 10*time.Second),
			"refused instead of retried forever")
		assert.Empty(t, n.read(t, mid))
	})
}

func countCustomersIn(customers []string, part, count int) int {
	n := 0
	for _, c := range customers {
		if partition.Of(c, count) == part {
			n++
		}
	}
	return n
}

func testBrokerBuffer(t *testing.T, n *natsServer) {
	t.Helper()
	const messages, workers = 200, 20
	mid := n.brokerStream(t, "waiting")
	publishLogs(t, n, mid+".logs", messages)

	in := n.brokerSource(mid, "logs", mid+".>")
	in.Spec["workers"] = float64(workers)
	held := captureSinkSpec("received", "in")
	held.Spec = map[string]any{"hold": true}
	forward := newStream(t, in, nil, []domain.SinkSpec{held})
	received := forward.sinks.sink(t, "received")
	t.Cleanup(received.release)

	var cons jetstream.Consumer
	var err error
	require.Eventually(t, func() bool {
		cons, err = n.js.Consumer(t.Context(), mid, mid)
		return err == nil
	}, 10*time.Second, 5*time.Millisecond, "the source did not create its consumer")

	var infoMu sync.Mutex
	counts := func() (int, uint64) {
		infoMu.Lock()
		defer infoMu.Unlock()
		info, infoErr := cons.Info(t.Context())
		if infoErr != nil {
			return -1, 0
		}
		return info.NumAckPending, info.NumPending
	}
	require.Eventually(t, func() bool {
		out, _ := counts()
		return out >= workers
	}, 10*time.Second, 5*time.Millisecond, "the workers did not all receive a message")
	// Besides the messages being handled, the driver buffers at most one more
	// per worker.
	assert.Never(t, func() bool {
		out, _ := counts()
		return out > 2*workers
	}, 500*time.Millisecond, 10*time.Millisecond, "more messages were out than workers and buffer hold")
	_, waiting := counts()
	assert.GreaterOrEqual(t, waiting, uint64(messages-2*workers), "the other messages wait in the stream")

	received.release()
	require.Eventually(t, func() bool {
		out, left := counts()
		return out == 0 && left == 0
	}, 20*time.Second, 5*time.Millisecond, "the messages were not acknowledged")
}

// brokerStream creates a stream for name's subjects, as done outside TRAM.
func (n *natsServer) brokerStream(t *testing.T, name string) string {
	t.Helper()
	stream := n.stream(name)
	_, err := n.js.CreateStream(t.Context(), jetstream.StreamConfig{Name: stream, Subjects: []string{stream + ".>"}})
	require.NoError(t, err)
	return stream
}

// brokerSinkSpec is a broker sink spec with the NATS driver; extra are pairs
// of further fields and values.
func (n *natsServer) brokerSinkSpec(destination map[string]any, extra ...any) map[string]any {
	spec := map[string]any{
		"driver":      "nats",
		"nats":        map[string]any{"url": n.url},
		"destination": destination,
	}
	for i := 0; i+1 < len(extra); i += 2 {
		spec[extra[i].(string)] = extra[i+1]
	}
	return spec
}

func (n *natsServer) brokerSinkByService(name, from, stream string) domain.SinkSpec {
	return domain.SinkSpec{Name: name, Type: "broker", From: from, Spec: n.brokerSinkSpec(map[string]any{
		"logs": fmt.Sprintf("'%s.' + logs.resource_logs[0].resource.attributes.get_attr('service.name')", stream),
	})}
}

// brokerSource reads signal from subject of stream with a durable consumer
// named after the stream and subject.
func (n *natsServer) brokerSource(stream, signal, subject string) domain.SourceSpec {
	durable := strings.NewReplacer(".", "_", ">", "all", "*", "any", "{", "", "}", "").Replace(subject)
	if strings.HasSuffix(subject, ".>") {
		durable = strings.NewReplacer("{", "", "}", "").Replace(stream)
	}
	return domain.SourceSpec{Name: "in", Type: "broker", Spec: map[string]any{
		"driver": "nats",
		"signal": signal,
		"nats":   map[string]any{"url": n.url, "stream": stream, "subject": subject, "durable": durable},
	}}
}

func publishLogs(t *testing.T, n *natsServer, subject string, count int) {
	t.Helper()
	body, err := proto.Marshal(otlpgen.Logs(otlpgen.Shape{Records: 1, Resources: 1}))
	require.NoError(t, err)
	for range count {
		_, err = n.js.PublishMsg(t.Context(), &nats.Msg{Subject: subject, Data: body})
		require.NoError(t, err)
	}
}

// logsBySubject decodes messages without headers as logs.
func logsBySubject(t *testing.T, messages []*jetstream.RawStreamMsg) map[string][]string {
	t.Helper()
	out := map[string][]string{}
	for _, m := range messages {
		event, err := domain.UnmarshalOTLPData(domain.SignalLogs, m.Data)
		require.NoError(t, err)
		out[m.Subject] = append(out[m.Subject], arrived([]*domain.Event{event})...)
	}
	for subject := range out {
		sort.Strings(out[subject])
	}
	return out
}

// testOneSourcePerSignal sends each signal to its own subject and reads it with a source of that signal.
func testOneSourcePerSignal(t *testing.T, n *natsServer) {
	t.Helper()
	mid := n.brokerStream(t, "signals")
	ingest := newPipeline(t, nil, []domain.SinkSpec{{
		Name: "out", Type: "broker", From: "in", Spec: n.brokerSinkSpec(map[string]any{
			"logs":    fmt.Sprintf("'%s.logs'", mid),
			"metrics": fmt.Sprintf("'%s.metrics'", mid),
			"traces":  fmt.Sprintf("'%s.traces'", mid),
		}),
	}})
	received := map[string]*captureSink{}
	for _, signal := range []string{"logs", "metrics", "traces"} {
		s := newStream(t, n.brokerSource(mid, signal, mid+"."+signal),
			nil, []domain.SinkSpec{captureSinkSpec("received", "in")})
		received[signal] = s.sinks.sink(t, "received")
	}

	shape := otlpgen.Shape{Records: 2, Resources: 1, Teams: 1}
	require.Equal(t, http.StatusOK, ingest.postTo(t, "/v1/logs", otlpgen.Logs(shape), 10*time.Second))
	require.Equal(t, http.StatusOK, ingest.postTo(t, "/v1/metrics", otlpgen.Metrics(shape), 10*time.Second))
	require.Equal(t, http.StatusOK, ingest.postTo(t, "/v1/traces", otlpgen.Traces(shape), 10*time.Second))

	for signal, sink := range received {
		require.Eventually(t, func() bool { return len(sink.received()) == 1 }, 10*time.Second, 10*time.Millisecond,
			"the %s source", signal)
		e := sink.received()[0]
		assert.Equal(t, 2, e.RecordCount(), signal)
		switch signal {
		case "logs":
			assert.NotNil(t, e.Logs)
		case "metrics":
			assert.NotNil(t, e.Metrics)
		case "traces":
			assert.NotNil(t, e.Traces)
		}
	}
}

// testPartitionedByCustomer writes to partitioned streams and reads each customer from its partition.
func testPartitionedByCustomer(t *testing.T, n *natsServer) {
	t.Helper()
	const count = 3
	prefix := n.stream("part")
	for p := range count {
		name := fmt.Sprintf("%s-%d", prefix, p)
		_, err := n.js.CreateStream(t.Context(), jetstream.StreamConfig{
			Name: name, Subjects: []string{fmt.Sprintf("%s.%d.>", prefix, p)},
		})
		require.NoError(t, err)
	}
	ingest := newPipeline(t, nil, []domain.SinkSpec{{
		Name: "out", Type: "broker", From: "in",
		Spec: n.brokerSinkSpec(map[string]any{
			"logs": fmt.Sprintf("'%s.' + string(partition) + '.' + record.attributes.get_attr('%s')",
				prefix, customerAttr),
		}, "partition", map[string]any{
			"count": float64(count), "logs": "record.attributes.get_attr('" + customerAttr + "')",
		}),
	}})
	customers := []string{"A", "B", "C", "D"}
	received := map[string]*captureSink{}
	for _, customer := range customers {
		src := n.brokerSource(prefix+"-{partition}", "logs", prefix+".{partition}."+customer)
		src.Spec["partition"] = map[string]any{"count": float64(count), "key": customer}
		s := newStream(t, src, nil, []domain.SinkSpec{captureSinkSpec("received", "in")})
		received[customer] = s.sinks.sink(t, "received")
	}

	var records []rec
	for _, customer := range customers {
		records = append(records, rec{"r-" + customer, customer})
	}
	require.Equal(t, http.StatusOK, ingest.post(t, request("shop", records...), 10*time.Second))

	for _, customer := range customers {
		require.Eventually(t, func() bool { return len(arrived(received[customer].received())) == 1 },
			10*time.Second, 10*time.Millisecond, "customer %s", customer)
		assert.Equal(t, []string{"shop/e2e/r-" + customer}, arrived(received[customer].received()))
		stream := fmt.Sprintf("%s-%d", prefix, partition.Of(customer, count))
		assert.Len(t, n.read(t, stream), countCustomersIn(customers, partition.Of(customer, count), count),
			"stream %s holds the customers of its partition", stream)
	}
}
