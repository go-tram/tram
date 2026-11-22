// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

// Package sqlite is the sqlite driver of the buffer sink and source: it
// keeps every queue in one SQLite file in the data directory of the instance,
// written with a write-ahead log and synced on every commit. It uses
// modernc.org/sqlite, which needs no cgo.
//
// The file of a queue is locked by a lock file next to it, so a second
// process cannot use it while the first holds it. Within a process, all
// publishers and subscribers of one queue share it. Payloads are kept in a
// table of their own, so scheduling a retry rewrites a few bytes instead of
// the payload.
package sqlite

import (
	"context"
	"fmt"
	"log/slog"
	"path/filepath"
	"regexp"
	"time"

	"github.com/go-playground/validator/v10"

	"github.com/go-tram/tram/internal/adapters/broker"
	"github.com/go-tram/tram/internal/adapters/buffer"
	"github.com/go-tram/tram/internal/core/domain"
)

// Name is the driver name of buffer specs.
const Name = "sqlite"

// Spec is the options of the sqlite driver. The queue's file is
// <data directory>/<queue>.db.
type Spec struct{}

// Driver opens buffer queues in a data directory.
type Driver struct {
	validator *validator.Validate
	dataDir   string
	now       func() time.Time
}

var _ buffer.Driver = (*Driver)(nil)

// New returns the driver for queue files in dataDir. Without a data
// directory, no buffer can run on the instance.
func New(validator *validator.Validate, dataDir string) *Driver {
	return &Driver{validator: validator, dataDir: dataDir, now: time.Now}
}

var queuePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]*$`)

// validateQueue checks a queue name: the file it names stays in the data
// directory.
func validateQueue(queue string) error {
	if !queuePattern.MatchString(queue) || !filepath.IsLocal(queue+".db") {
		return fmt.Errorf("queue %q may only hold letters, digits and . _ -, starting with a letter or digit", queue)
	}
	return nil
}

// pathOf is the file of a queue.
func (d *Driver) pathOf(queue string) (string, error) {
	if err := validateQueue(queue); err != nil {
		return "", err
	}
	return filepath.Join(d.dataDir, queue+".db"), nil
}

// queueName is the queue of a file path.
func queueName(path string) string {
	name := filepath.Base(path)
	return name[:len(name)-len(".db")]
}

// NewPublisher opens the queue the config names and writes to it.
func (d *Driver) NewPublisher(
	opts map[string]any,
	cfg buffer.PublisherConfig,
	logger *slog.Logger,
) (broker.Publisher, error) {
	if _, err := domain.ParseSpec(opts, &Spec{}); err != nil {
		return nil, err
	}
	if d.dataDir == "" {
		return nil, buffer.ErrNoDataDir
	}
	path, err := d.pathOf(cfg.Queue)
	if err != nil {
		return nil, err
	}
	// Hold the queue's file for the publisher's lifetime, so a second process
	// is refused from the start, and record the max_age with it.
	q, err := queues.acquire(path, d.now, logger)
	if err != nil {
		return nil, err
	}
	if cfg.MaxAge > 0 {
		if err = q.store.setMaxAge(context.Background(), cfg.MaxAge); err != nil {
			queues.release(path)
			return nil, fmt.Errorf("record max_age of queue %s: %w", cfg.Queue, err)
		}
	}
	return &publisher{
		drv:      d,
		maxBytes: cfg.MaxBytes,
		logger:   logger,
		paths:    map[string]int{path: 1},
	}, nil
}

// NewSubscriber opens the queue the config names and reads from it.
func (d *Driver) NewSubscriber(
	opts map[string]any,
	cfg buffer.SubscriberConfig,
	logger *slog.Logger,
) (broker.Subscriber, error) {
	if _, err := domain.ParseSpec(opts, &Spec{}); err != nil {
		return nil, err
	}
	if d.dataDir == "" {
		return nil, buffer.ErrNoDataDir
	}
	path, err := d.pathOf(cfg.Queue)
	if err != nil {
		return nil, err
	}
	q, err := queues.acquire(path, d.now, logger)
	if err != nil {
		return nil, err
	}
	return &subscriber{queue: q, path: path, unread: q.addReader()}, nil
}

// Files lists the queue files in the data directory with what they hold,
// without opening them, so a queue in use is not disturbed. Without a data
// directory, there are none.
func (d *Driver) Files(ctx context.Context) ([]buffer.File, error) {
	if d.dataDir == "" {
		return nil, nil
	}
	paths, err := queuePaths(d.dataDir)
	if err != nil {
		return nil, err
	}
	files := make([]buffer.File, 0, len(paths))
	for _, path := range paths {
		info, inspectErr := inspect(ctx, path)
		files = append(files, buffer.File{
			Path:  path,
			InUse: queues.inUse(path),
			Info:  info,
			Err:   inspectErr,
		})
	}
	return files, nil
}
