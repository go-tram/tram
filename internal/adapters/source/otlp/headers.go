// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package otlp

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"slices"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/go-tram/tram/internal/core/domain"
)

// headerMapping copies a request header, or the gRPC metadata of the same
// name, into a resource attribute of every resource of the request. Headers
// are set by whoever forwards the request, e.g. an ingress per tenant, so the
// mapping makes such a property of the request part of its data, where stages
// and sinks read it and where it survives a broker between streams, whose
// messages carry no headers.
type headerMapping struct {
	// header is the canonical HTTP name, metadata the lower-case gRPC key.
	header, metadata string
	attribute        string
}

var (
	errInvalidHeaderName = errors.New("invalid header name")
	errHeaderRepeated    = errors.New("header appears more than once")
)

// headerName is an HTTP field name (RFC 9110, token).
var headerName = regexp.MustCompile("^[!#$%&'*+.^_`|~0-9A-Za-z-]+$")

// newHeaderMappings checks the header_attributes of a spec and orders them,
// so every request is changed the same way.
func newHeaderMappings(spec map[string]string) ([]headerMapping, error) {
	mappings := make([]headerMapping, 0, len(spec))
	headers := map[string]string{}
	attributes := map[string]string{}
	for header, attribute := range spec {
		if !headerName.MatchString(header) {
			return nil, fmt.Errorf("header_attributes: %w %q", errInvalidHeaderName, header)
		}
		if attribute == "" {
			return nil, fmt.Errorf("header_attributes: header %q needs an attribute key", header)
		}
		canonical := http.CanonicalHeaderKey(header)
		if other, ok := headers[canonical]; ok {
			return nil, fmt.Errorf("header_attributes: headers %q and %q are the same header", other, header)
		}
		if other, ok := attributes[attribute]; ok {
			return nil, fmt.Errorf(
				"header_attributes: headers %q and %q both set attribute %q",
				other,
				header,
				attribute,
			)
		}
		headers[canonical], attributes[attribute] = header, header
		mappings = append(mappings, headerMapping{
			header: canonical, metadata: strings.ToLower(header), attribute: attribute,
		})
	}
	slices.SortFunc(mappings, func(a, b headerMapping) int { return strings.Compare(a.header, b.header) })
	return mappings, nil
}

// applyHeaders sets the attributes of the mappings on every resource of the
// event, from the values values returns for a mapping. The header decides
// the attribute alone: a value the client put into the attribute itself is
// replaced, and removed when the header is missing or empty, so a client
// cannot claim an attribute that the forwarding side sets. A header with
// several values is refused, since it is unclear which one the forwarding
// side meant, e.g. when it appended its value to the client's header instead
// of replacing it.
func applyHeaders(event *domain.Event, mappings []headerMapping, values func(headerMapping) []string) error {
	for _, m := range mappings {
		vals := values(m)
		if len(vals) > 1 {
			return fmt.Errorf("%w: %s", errHeaderRepeated, m.header)
		}
		value := ""
		if len(vals) == 1 {
			value = vals[0]
		}
		forEachResource(event, func(r **resourcepb.Resource) { setResourceAttribute(r, m.attribute, value) })
	}
	return nil
}

func forEachResource(event *domain.Event, f func(**resourcepb.Resource)) {
	for _, rl := range event.Logs.GetResourceLogs() {
		f(&rl.Resource)
	}
	for _, rm := range event.Metrics.GetResourceMetrics() {
		f(&rm.Resource)
	}
	for _, rs := range event.Traces.GetResourceSpans() {
		f(&rs.Resource)
	}
}

// setResourceAttribute replaces key on the resource with a string value, or
// removes it for an empty value. A missing resource is created for a value.
func setResourceAttribute(r **resourcepb.Resource, key, value string) {
	if *r == nil {
		if value == "" {
			return
		}
		*r = &resourcepb.Resource{}
	}
	(*r).Attributes = slices.DeleteFunc((*r).GetAttributes(), func(kv *commonpb.KeyValue) bool {
		return kv.GetKey() == key
	})
	if value != "" {
		(*r).Attributes = append((*r).GetAttributes(), &commonpb.KeyValue{
			Key:   key,
			Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}},
		})
	}
}
