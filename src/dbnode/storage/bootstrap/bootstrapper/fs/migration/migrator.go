// Copyright (c) 2020 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package migration

import (
	"sync"

	"github.com/m3db/m3/src/dbnode/persist"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs/migration"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/fs"
)

// workerArgs contains all the information a worker go-routine needs to
// perform a migration
type workerArgs struct {
	namespace namespace.Metadata
	shard     uint32
	sOpts     storage.Options
}

type worker struct {
	inputCh        chan workerArgs
	persistManager persist.Manager
}

const workerChannelSize = 256

// Migrator is an object responsible for migrating data filesets based on version information in
// the info files.
type Migrator struct {
	infoFilesByNamespace map[namespace.Metadata]fs.ShardsInfoFilesResult
	opts                 migration.Options
	sOpts                storage.Options
}

// NewMigrator creates a new Migrator.
func NewMigrator(infoFilesByNamespace map[namespace.Metadata]fs.ShardsInfoFilesResult, opts migration.Options) Migrator {
	return Migrator{
		infoFilesByNamespace: infoFilesByNamespace,
		opts:                 opts,
	}
}

// TODO(nate): add some logging and error reporting?

// Run runs the migrator
func (m *Migrator) Run() error {
	// TODO(nate): is this the best way to check?
	if !m.opts.ToVersion1_1() {
		return nil
	}

	// Setup workers to perform migrations
	var (
		numWorkers = m.opts.Concurrency()
		workers    = make([]*worker, 0, numWorkers)
	)

	for i := 0; i < numWorkers; i++ {
		worker := &worker{
			inputCh: make(chan workerArgs, workerChannelSize),
		}
		workers = append(workers, worker)
	}
	closedWorkerChannels := false
	closeWorkerChannels := func() {
		if closedWorkerChannels {
			return
		}
		closedWorkerChannels = true
		for _, worker := range workers {
			close(worker.inputCh)
		}
	}
	// NB(nate): Ensure that channels always get closed.
	defer closeWorkerChannels()

	// Start up workers
	var wg sync.WaitGroup
	for _, worker := range workers {
		worker := worker
		wg.Add(1)
		go func() {
			m.startWorker(worker)
			wg.Done()
		}()
	}

	// Enqueue work for workers
	enqueued := 0
	for md, resultsByShard := range m.infoFilesByNamespace {
		for shard, results := range resultsByShard {
			for _, info := range results {
				// TODO(nate): replace with check from migration task
				if info.Info.MajorVersion == 1 && info.Info.MinorVersion == 1 {
					enqueued++
					worker := workers[numWorkers%enqueued]
					worker.inputCh <- workerArgs{
						namespace: md,
						shard:     shard,
						sOpts:     m.sOpts,
					}
				}
			}
		}
	}

	wg.Wait()

	return nil
}

func (m *Migrator) startWorker(worker *worker) {
	for input := range worker.inputCh {
		// TODO: run migrator task
	}
}
