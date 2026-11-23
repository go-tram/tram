// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package configsync

import (
	"context"
	"time"
)

// SetRetryDelays shortens the delays between retries of streams that could
// not be applied. It must be called before Run.
func SetRetryDelays(c *ConfigSync, minDelay, maxDelay time.Duration) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	c.minRetryDelay, c.maxRetryDelay = minDelay, maxDelay
}

// Sync syncs the directory once, like a change of a file does.
func Sync(ctx context.Context, c *ConfigSync) (int, error) {
	return c.sync(ctx)
}

// Running tells whether Run receives the changes of the core.
func Running(c *ConfigSync) bool {
	return c.running.Load()
}
