// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package api

import (
	"github.com/go-tram/tram/internal/core/domain"
	v1 "github.com/go-tram/tram/pkg/gen/go/tram/v1"
)

func ToStatus(err error) error {
	return toStatus(err)
}

func PBStreamToStreamSpec(stream *v1.Stream) domain.StreamSpec {
	return pbStreamToStreamSpec(stream)
}

func StreamSpecToPBStream(spec domain.StreamSpec) *v1.Stream {
	return streamSpecToPBStream(spec)
}
