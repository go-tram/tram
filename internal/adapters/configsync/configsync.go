// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at https://mozilla.org/MPL/2.0/.
// SPDX-License-Identifier: MPL-2.0

package configsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fsnotify/fsnotify"
	"golang.org/x/sync/errgroup"

	"github.com/go-tram/tram/internal/core/domain"
	"github.com/go-tram/tram/internal/ports"
)

const (
	fsEventBuffer    = 100
	debounceDuration = 100 * time.Millisecond
	filePermissions  = 0o600
	fileExtension    = ".json"

	// Streams that could not be applied, e.g. because NATS was not up yet
	// when Tram started, are tried again after a delay that doubles with
	// every failed sync, between these bounds.
	minRetryDelay    = time.Second
	maxRetryDelay    = 30 * time.Second
	retryDelayGrowth = 2
	// failureLogInterval bounds how often a stream that keeps failing is
	// logged, so hundreds of them do not flood the log on every retry.
	failureLogInterval = time.Minute
	// syncWorkers bounds how many streams a sync applies or deletes at once.
	// Building a stream can take long, e.g. while its sinks connect to a
	// broker, so a directory with many streams loads in parallel.
	syncWorkers = 8
)

type ConfigSync struct {
	dir              string
	core             ports.Core
	logger           *slog.Logger
	watcher          *fsnotify.Watcher
	fsEvents         chan fsnotify.Event
	debouncer        *time.Timer
	debounceDuration time.Duration
	syncMu           sync.Mutex
	// written asks the loop of Run to report, by closing the channel it
	// receives, that it has written the files of every change of the core it
	// received before; see waitForWrites.
	written chan chan struct{}
	// running tells whether the loop of Run receives the changes of the core.
	running atomic.Bool

	// Guarded by syncMu.
	unapplied                    int
	retry                        *time.Timer
	retryDelay                   time.Duration
	minRetryDelay, maxRetryDelay time.Duration
	// failing holds when a stream that could not be applied was last logged.
	failing map[string]time.Time
}

func New(c ports.Core, dir string, logger *slog.Logger) (*ConfigSync, error) {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return nil, fmt.Errorf("failed to create file watcher: %w", err)
	}

	if err = watcher.Add(dir); err != nil {
		return nil, errors.Join(fmt.Errorf("failed to watch directory %q: %w", dir, err), watcher.Close())
	}

	cs := &ConfigSync{
		dir:              dir,
		core:             c,
		logger:           logger,
		watcher:          watcher,
		fsEvents:         make(chan fsnotify.Event, fsEventBuffer),
		written:          make(chan chan struct{}),
		debounceDuration: debounceDuration,
		minRetryDelay:    minRetryDelay,
		maxRetryDelay:    maxRetryDelay,
		failing:          map[string]time.Time{},
	}

	// Streams that cannot be applied yet are tried again once Run starts.
	if _, err = cs.sync(context.Background()); err != nil {
		return nil, errors.Join(err, watcher.Close())
	}

	return cs, nil
}

func (c *ConfigSync) Run(ctx context.Context) error {
	c.logger.InfoContext(ctx, "Starting configuration sync", slog.String("directory", c.dir))

	// Cancelling on return unregisters the watch, so the core never blocks
	// on a channel nobody reads anymore. Derived before the goroutine below
	// starts, which reads ctx.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go c.forwardFSEvents(ctx)

	ch := make(chan domain.StreamSpecWatchEvent)
	if err := c.core.Watch(ctx, ch); err != nil {
		return fmt.Errorf("failed to watch core: %w", err)
	}

	defer func() {
		if c.debouncer != nil {
			c.debouncer.Stop()
		}
		c.syncMu.Lock()
		if c.retry != nil {
			c.retry.Stop()
		}
		c.syncMu.Unlock()
	}()

	c.syncMu.Lock()
	unapplied := c.unapplied
	c.syncMu.Unlock()
	c.afterSync(ctx, unapplied, nil, false)

	c.running.Store(true)
	// Runs first on return: a sync waiting for the loop stops waiting before
	// the deferred cleanup above takes syncMu, which that sync holds.
	defer func() {
		c.running.Store(false)
		cancel()
	}()
	for {
		select {
		case <-ctx.Done():
			return nil

		case done := <-c.written:
			close(done)

		case err := <-c.watcher.Errors:
			return fmt.Errorf("watcher error: %w", err)

		case event := <-ch:
			if err := c.onCoreChange(event); err != nil {
				c.logger.ErrorContext(ctx, "Could not write stream change to file", slog.Any("error", err))
			}

		case event := <-c.fsEvents:
			c.scheduleSync(ctx, event)
		}
	}
}

func (c *ConfigSync) forwardFSEvents(ctx context.Context) {
	for event := range c.watcher.Events {
		c.logger.DebugContext(ctx, "Received file system event",
			slog.String("path", event.Name), slog.String("op", event.Op.String()))
		select {
		case c.fsEvents <- event:
		case <-ctx.Done():
			return
		}
	}
}

func (c *ConfigSync) scheduleSync(ctx context.Context, event fsnotify.Event) {
	if filepath.Ext(event.Name) != fileExtension {
		return
	}
	if c.debouncer != nil {
		c.debouncer.Stop()
	}
	c.debouncer = time.AfterFunc(c.debounceDuration, func() {
		unapplied, err := c.sync(ctx)
		c.afterSync(ctx, unapplied, err, true)
	})
}

// afterSync schedules another sync if streams could not be applied or the
// sync failed. A sync after a change to the files starts over with the
// shortest delay.
func (c *ConfigSync) afterSync(ctx context.Context, unapplied int, err error, filesChanged bool) {
	if err != nil && ctx.Err() == nil {
		c.logger.ErrorContext(ctx, "Could not sync stream files", slog.Any("error", err))
	}

	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	if ctx.Err() != nil {
		return
	}
	if c.retry != nil {
		c.retry.Stop()
		c.retry = nil
	}
	if filesChanged || (unapplied == 0 && err == nil) {
		c.retryDelay = 0
	}
	if unapplied == 0 && err == nil {
		return
	}

	c.retryDelay = min(max(retryDelayGrowth*c.retryDelay, c.minRetryDelay), c.maxRetryDelay)
	c.logger.DebugContext(ctx, "Trying stream files again",
		slog.Int("unapplied", unapplied), slog.Duration("delay", c.retryDelay))
	c.retry = time.AfterFunc(c.retryDelay, func() {
		stillUnapplied, syncErr := c.sync(ctx)
		c.afterSync(ctx, stillUnapplied, syncErr, false)
	})
}

// A stream whose file cannot be read is kept: deleting it would also delete
// the file, e.g. while an editor is still saving it. Syncs run one at a time,
// so a slow sync cannot apply an older state of the files after a newer one.
// sync returns how many streams could not be applied.
//
// Streams are created through the API or the operator while a sync runs, and
// their files are written after they run. So sync lists the running streams
// before it reads the files, waits until the files of these streams are
// written, and deletes only streams of that list: a stream created after it
// is not in it, and one created before has its file.
func (c *ConfigSync) sync(ctx context.Context) (int, error) {
	c.syncMu.Lock()
	defer c.syncMu.Unlock()
	if err := ctx.Err(); err != nil {
		return 0, err
	}

	configured, err := c.core.GetStreams(ctx)
	if err != nil {
		return 0, err
	}
	if err = c.waitForWrites(ctx); err != nil {
		return 0, err
	}

	files, unreadable, err := c.readStreamFiles(ctx)
	if err != nil {
		return 0, err
	}

	c.deleteRemovedStreams(ctx, configured, files, unreadable)

	for name := range c.failing {
		if _, ok := files[name]; !ok {
			delete(c.failing, name)
		}
	}

	unapplied := c.applyStreamFiles(ctx, files)
	c.unapplied = unapplied
	return unapplied, nil
}

// waitForWrites returns once the loop of Run has written the files of all
// changes of the core it received so far. The core sends a change before the
// stream can be listed, so afterwards every listed stream has its file. Before
// Run, nothing writes files and there is nothing to wait for.
func (c *ConfigSync) waitForWrites(ctx context.Context) error {
	if !c.running.Load() {
		return nil
	}
	done := make(chan struct{})
	select {
	case c.written <- done:
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// deleteRemovedStreams deletes the configured streams that have no file
// anymore. Streams whose file cannot be read are kept, as are streams that
// were deleted meanwhile. Must be called with syncMu held.
func (c *ConfigSync) deleteRemovedStreams(
	ctx context.Context,
	configured []domain.StreamSpec,
	files map[string]Stream,
	unreadable map[string]struct{},
) {
	var g errgroup.Group
	g.SetLimit(syncWorkers)
	for _, stream := range configured {
		if _, ok := files[stream.Name]; ok {
			continue
		}
		if _, ok := unreadable[stream.Name]; ok {
			c.logger.WarnContext(ctx, "Keeping stream whose file cannot be read",
				slog.String("stream", stream.Name))
			continue
		}
		g.Go(func() error {
			c.logger.DebugContext(ctx, "Deleting stream", slog.String("stream", stream.Name))
			if err := c.core.DeleteStream(ctx, stream.Name); err != nil && !errors.Is(err, domain.ErrStreamNotFound) {
				c.logger.ErrorContext(
					ctx,
					"Could not delete stream",
					slog.String("stream", stream.Name),
					slog.Any("error", err),
				)
			}
			return nil
		})
	}
	_ = g.Wait()
}

// applyStreamFiles applies every stream file, up to syncWorkers at once, and
// returns how many could not be applied. Must be called with syncMu held.
func (c *ConfigSync) applyStreamFiles(ctx context.Context, files map[string]Stream) int {
	errs := make(map[string]error, len(files))
	var mu sync.Mutex
	var g errgroup.Group
	g.SetLimit(syncWorkers)
	for name, file := range files {
		g.Go(func() error {
			err := c.apply(ctx, name, file)
			mu.Lock()
			errs[name] = err
			mu.Unlock()
			return nil
		})
	}
	_ = g.Wait()

	unapplied := 0
	for name, err := range errs {
		if err != nil {
			unapplied++
			c.logFailure(ctx, name, err)
			continue
		}
		if _, failed := c.failing[name]; failed {
			c.logger.InfoContext(ctx, "Applied stream that could not be applied before", slog.String("stream", name))
			delete(c.failing, name)
		}
	}
	return unapplied
}

// logFailure logs a stream that could not be applied, the first time and then
// at most every failureLogInterval. Must be called with syncMu held.
func (c *ConfigSync) logFailure(ctx context.Context, name string, err error) {
	last, failed := c.failing[name]
	if failed && time.Since(last) < failureLogInterval {
		return
	}
	c.failing[name] = time.Now()
	c.logger.ErrorContext(ctx, "Could not apply stream, trying again later",
		slog.String("stream", name), slog.Any("error", err))
}

func (c *ConfigSync) readStreamFiles(ctx context.Context) (map[string]Stream, map[string]struct{}, error) {
	entries, err := os.ReadDir(c.dir)
	if err != nil {
		return nil, nil, err
	}

	files := make(map[string]Stream, len(entries))
	unreadable := map[string]struct{}{}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != fileExtension {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), fileExtension)
		stream, readErr := c.StreamFromFile(entry.Name())
		if readErr != nil {
			c.logger.ErrorContext(ctx, "Could not read stream file",
				slog.String("file", entry.Name()), slog.Any("error", readErr))
			unreadable[name] = struct{}{}
			continue
		}
		stream.Stream.Name = name
		files[name] = stream
	}
	return files, unreadable, nil
}

func (c *ConfigSync) apply(ctx context.Context, name string, file Stream) error {
	configured, err := c.core.GetStream(ctx, name)
	if err != nil && !errors.Is(err, domain.ErrStreamNotFound) {
		return fmt.Errorf("get stream: %w", err)
	}
	if configured != nil && file.Equals(StreamFromDomain(*configured)) {
		return nil
	}

	c.logger.DebugContext(ctx, "Upserting stream", slog.String("stream", name))
	if _, err = c.core.UpsertStream(ctx, *file.AsDomain()); err != nil {
		return fmt.Errorf("upsert stream: %w", err)
	}
	return nil
}

func (c *ConfigSync) onCoreChange(event domain.StreamSpecWatchEvent) error {
	fileName := event.StreamSpec.Name + fileExtension
	if !filepath.IsLocal(fileName) || filepath.Base(fileName) != fileName {
		return fmt.Errorf("stream %q has no file name inside %s", event.StreamSpec.Name, c.dir)
	}
	path := filepath.Join(c.dir, fileName)

	switch event.EventType {
	case domain.StreamSpecWatchEventTypeDelete:
		err := os.Remove(path)
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return err

	case domain.StreamSpecWatchEventTypeUpsert:
		stored, err := c.StreamFromFile(fileName)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}

		stream := StreamFromDomain(event.StreamSpec)
		if stored.Equals(stream) {
			return nil
		}

		content, err := json.MarshalIndent(stream, "", "  ")
		if err != nil {
			return err
		}
		return os.WriteFile(path, content, filePermissions)
	}
	return nil
}

func (c *ConfigSync) StreamFromFile(fileName string) (Stream, error) {
	content, err := os.ReadFile(filepath.Join(c.dir, fileName))
	if err != nil {
		return Stream{}, err
	}

	var stream Stream
	if err = json.Unmarshal(content, &stream); err != nil {
		return Stream{}, fmt.Errorf("failed to parse %s: %w", fileName, err)
	}
	return stream, nil
}

func (c *ConfigSync) Close() error {
	if c.watcher == nil {
		return nil
	}
	return c.watcher.Close()
}
