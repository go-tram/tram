// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package domain

import "errors"

var (
	ErrStageDuplicate       = errors.New("stage name used twice")
	ErrStageInvalidFrom     = errors.New("stage from is unknown to stream")
	ErrSinkDuplicate        = errors.New("sink name used twice")
	ErrSinkInvalidFrom      = errors.New("sink from is unknown to stream")
	ErrGraphNotTree         = errors.New("stream is not a Tree")
	ErrGraphNotConnected    = errors.New("stream is not a connected graph")
	ErrGraphLeafNotSink     = errors.New("stream has a leaf that is not a sink")
	ErrGraphSinkHasChildren = errors.New("stream has a sink that has children")
	ErrGraphHasNoRoot       = errors.New("stream has no root")
	ErrInvalidStreamSpec    = errors.New("stream spec is invalid")
	ErrInvalidSourceType    = errors.New("source type is invalid")
	ErrInvalidStageType     = errors.New("stage type is invalid")
	ErrInvalidSinkType      = errors.New("sink type is invalid")
	ErrStreamWireStages     = errors.New("cannot wire stage")
	ErrStreamWireSinks      = errors.New("cannot wire sink")
	ErrStreamNotFound       = errors.New("stream not found")
	ErrInvalidStreamName    = errors.New("stream name is invalid")
	ErrStreamStopFailed     = errors.New("stream stop failed")
	ErrStreamWireFailed     = errors.New("stream wire failed")
	ErrStreamBuildFailed    = errors.New("stream build failed")
	ErrCoreClosed           = errors.New("core closed")
	ErrNodeNotSource        = errors.New("node is not a source")
	ErrNodeNotStage         = errors.New("node is not a stage")
	ErrNodeNotSink          = errors.New("node is not a sink")
	ErrNodeNotObserver      = errors.New("node is not an observer")
	ErrNodeNotSubscriber    = errors.New("node is not a subscriber")
	ErrPortUnavailable      = errors.New("port unavailable")
	ErrEventRejected        = errors.New("event rejected")
	ErrComponentNotFound    = errors.New("component not found")
	ErrInvalidTapOptions    = errors.New("tap options are invalid")
)
