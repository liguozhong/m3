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
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage"
	xtime "github.com/m3db/m3/src/x/time"
)

// Migration interface is implemented by tasks that wish to perform a data migration
// on a fileset. Typically involves updating files in a fileset that were created by
// a previous version of the database client
type Migration interface {

	// Run is the set of steps to successfully complete a migration
	Run() error
}

var _ Migration = &ToVersion1_1{}

// ToVersion1_1 is an object responsible for migrating a fileset to version 1.1
type ToVersion1_1 struct {
	newMergerFn       fs.NewMergerFn
	infoFileResult    fs.ReadInfoFileResult
	shard             uint32
	namespaceMetadata namespace.Metadata
	persistManager    persist.Manager
	opts              storage.Options
}

func NewToVersion1_1(opts TaskOptions) Migration {
	return ToVersion1_1{
		newMergerFn:       opts.NewMergerFn(),
		infoFileResult:    opts.InfoFileResult(),
		shard:             opts.Shard(),
		namespaceMetadata: opts.NamespaceMetadata(),
		persistManager:    opts.PersistManager(),
		opts:              opts.StorageOptions(),
	}
}

// Run executes the steps to bring a fileset to Version 1.1
func (v ToVersion1_1) Run() error {
	reader, err := fs.NewReader(v.opts.BytesPool(), v.opts.CommitLogOptions().FilesystemOptions())
	if err != nil {
		return err
	}

	merger := v.newMergerFn(reader, v.opts.DatabaseBlockOptions().DatabaseBlockAllocSize(),
		v.opts.SegmentReaderPool(), v.opts.MultiReaderIteratorPool(),
		v.opts.IdentifierPool(), v.opts.EncoderPool(), v.opts.ContextPool(), v.namespaceMetadata.Options())

	volIndex := v.infoFileResult.Info.VolumeIndex
	fsID := fs.FileSetFileIdentifier{
		Namespace:   v.namespaceMetadata.ID(),
		Shard:       v.shard,
		BlockStart:  xtime.FromNanoseconds(v.infoFileResult.Info.BlockStart),
		VolumeIndex: volIndex,
	}

	nsCtx := namespace.NewContextFrom(v.namespaceMetadata)

	flushPersist, err := v.persistManager.StartFlushPersist()
	if err != nil {
		return err
	}

	// Intentionally use a noop merger here as we simply want to rewrite the same files with the current encoder which
	// will generate index files with the entry level checksums.
	close, err := merger.Merge(fsID, storage.NewNoopMergeWith(), volIndex+1, flushPersist, nsCtx,
		&persist.NoOpColdFlushNamespace{})
	if err != nil {
		return err
	}

	if err := close(); err != nil {
		return err
	}

	return nil
}
