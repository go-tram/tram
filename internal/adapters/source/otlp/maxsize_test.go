// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp_test

import (
	"bytes"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"testing"

	"github.com/go-playground/validator/v10"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/go-tram/tram/internal/adapters/source/otlp"
	"github.com/go-tram/tram/internal/core/domain"
)

// testMaxRequestBytes is the max_request_bytes of the tests, far below the
// default.
const testMaxRequestBytes = 4096

func TestMaxRequestBytesOverHTTP(t *testing.T) {
	t.Parallel()
	small, err := proto.Marshal(logsRequest("small"))
	require.NoError(t, err)
	large, err := proto.Marshal(logsRequest(strings.Repeat("x", testMaxRequestBytes)))
	require.NoError(t, err)

	for name, tc := range map[string]struct {
		body     []byte
		encoding string
		status   int
		received int
	}{
		"below the limit":                {body: small, status: http.StatusOK, received: 1},
		"above the limit":                {body: large, status: http.StatusRequestEntityTooLarge},
		"above the limit when unzipped":  {body: gzipped(t, large), encoding: "gzip", status: http.StatusRequestEntityTooLarge},
		"below the limit, also unzipped": {body: gzipped(t, small), encoding: "gzip", status: http.StatusOK, received: 1},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rec := &syncRecorder{}
			port := freePort(t)
			startSource(t, map[string]any{
				"http_port": float64(port), "max_request_bytes": float64(testMaxRequestBytes),
			}, rec)

			req, reqErr := http.NewRequestWithContext(t.Context(), http.MethodPost,
				fmt.Sprintf("http://127.0.0.1:%d%s", port, logsPath), bytes.NewReader(tc.body))
			require.NoError(t, reqErr)
			req.Header.Set(contentTypeHeader, protobufContentType)
			if tc.encoding != "" {
				req.Header.Set("Content-Encoding", tc.encoding)
			}
			resp, reqErr := http.DefaultClient.Do(req)
			require.NoError(t, reqErr)
			_ = resp.Body.Close()

			assert.Equal(t, tc.status, resp.StatusCode)
			assert.Len(t, rec.received(), tc.received)
		})
	}
}

func TestMaxRequestBytesOverGRPC(t *testing.T) {
	t.Parallel()
	rec := &syncRecorder{}
	conn := grpcSource(t, rec, map[string]any{"max_request_bytes": float64(testMaxRequestBytes)})
	client := collogspb.NewLogsServiceClient(conn)

	_, err := client.Export(t.Context(), logsRequest(strings.Repeat("x", testMaxRequestBytes)))
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))
	assert.Empty(t, rec.received())

	_, err = client.Export(t.Context(), logsRequest("small"))
	require.NoError(t, err)
	assert.Len(t, rec.received(), 1)
}

func TestMaxRequestBytesIsValidated(t *testing.T) {
	t.Parallel()
	for name, value := range map[string]float64{
		"below 1 KiB": 1023,
		"negative":    -1,
		"above 1 GiB": 1<<30 + 1,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			_, err := otlp.New(domain.SourceSpec{Name: "otlp", Type: otlp.Type, Spec: map[string]any{
				"http_port": float64(freePort(t)), "max_request_bytes": value,
			}}, validator.New(), slog.New(slog.DiscardHandler))
			assert.ErrorContains(t, err, "MaxRequestBytes")
		})
	}
}
