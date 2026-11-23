// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package statements

import (
	"errors"
	"fmt"
	"regexp"
)

// Context is where a statement's target lives, and so how often it runs:
// once per resource, once per scope or once per entry.
type Context int

const (
	ContextEntry Context = iota
	ContextScope
	ContextResource
)

func (c Context) String() string {
	switch c {
	case ContextResource:
		return "resource"
	case ContextScope:
		return "scope"
	case ContextEntry:
		return "entry"
	default:
		return "entry"
	}
}

// Path is the target of a statement: an attribute of the entry, its scope or
// its resource.
type Path struct {
	Context Context
	Key     string
}

func (p Path) String() string {
	prefix := ""
	switch p.Context {
	case ContextResource:
		prefix = "resource."
	case ContextScope:
		prefix = "scope."
	case ContextEntry:
	}
	return fmt.Sprintf("%sattributes[%q]", prefix, p.Key)
}

// ErrInvalidPath is a target that is not one of the supported paths.
var ErrInvalidPath = errors.New(
	"invalid path: want attributes['key'], scope.attributes['key'] or resource.attributes['key']",
)

// pathPattern matches the supported paths, with the key in single or double
// quotes. Further paths, e.g. a log record's body, can be added later without
// changing these.
var pathPattern = regexp.MustCompile(`^(?:(resource|scope)\.)?attributes\[(?:'([^']+)'|"([^"]+)")\]$`)

// ParsePath parses a target path such as resource.attributes['service.name'].
func ParsePath(raw string) (Path, error) {
	m := pathPattern.FindStringSubmatch(raw)
	if m == nil {
		return Path{}, fmt.Errorf("%w, got %q", ErrInvalidPath, raw)
	}
	p := Path{Key: m[2] + m[3]}
	switch m[1] {
	case "resource":
		p.Context = ContextResource
	case "scope":
		p.Context = ContextScope
	}
	return p, nil
}
