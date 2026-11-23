// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package benchcpu measures the CPU time of the process during a benchmark,
// next to the wall time the benchmark itself measures. The two differ where a
// component waits, e.g. for a broker's acknowledgement: the wall time includes
// the wait, the CPU time only the work. CPU time covers every goroutine of the
// process, including the garbage collector and client libraries, but not
// processes outside it, such as a broker in a container.
package benchcpu

import (
	"syscall"
	"testing"
	"time"
)

// Unit is the unit of the metric Report adds, which benchreport reads.
const Unit = "cpu-ns/op"

// Meter holds the CPU time of the process when the measurement started.
type Meter struct {
	start time.Duration
}

// Start starts measuring; call it right before the timed part of a
// benchmark, e.g. before `for b.Loop()`.
func Start() Meter {
	return Meter{start: Now()}
}

// Report adds the CPU time since Start per operation as the metric Unit;
// call it right after the timed part.
func (m Meter) Report(b *testing.B) {
	b.Helper()
	if b.N == 0 {
		return
	}
	b.ReportMetric(float64((Now()-m.start).Nanoseconds())/float64(b.N), Unit)
}

// Now is the user and system CPU time the process has used so far.
func Now() time.Duration {
	var usage syscall.Rusage
	_ = syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano())
}
