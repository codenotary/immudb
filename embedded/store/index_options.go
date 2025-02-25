package store

import "time"

type IndexOptions struct {
	// Number of indexer threads
	NumIndexers int

	MinWriteBufferSize int

	MaxWriteBufferSize int

	PageBufferSize int

	SharedWriteBufferSize int

	WriteBufferChunkSize int

	BackpressureMinDelay time.Duration

	BackpressureMaxDelay time.Duration

	// Number of new index entries between disk flushes with file sync
	SyncThld int

	// Size of the in-memory flush buffer (in bytes)
	FlushBufferSize int

	// Percentage of node files cleaned up during each flush
	CleanupPercentage float32

	// Maximum number of active btree snapshots
	MaxActiveSnapshots int

	// Time between the most recent DB snapshot is automatically renewed
	RenewSnapRootAfter time.Duration

	// Percentage of stale pages to allow for full compaction
	CompactionThld float64

	// Additional delay added during indexing when full compaction is in progress
	DelayDuringCompaction time.Duration

	// Maximum number of simultaneously opened nodes files
	NodesLogMaxOpenedFiles int

	// Maximum number of simultaneously opened node history files
	HistoryLogMaxOpenedFiles int
}

func (opts *IndexOptions) WithSyncThld(syncThld int) *IndexOptions {
	opts.SyncThld = syncThld
	return opts
}

func (opts *IndexOptions) WithFlushBufferSize(flushBufferSize int) *IndexOptions {
	opts.FlushBufferSize = flushBufferSize
	return opts
}

func (opts *IndexOptions) WithCleanupPercentage(cleanupPercentage float32) *IndexOptions {
	opts.CleanupPercentage = cleanupPercentage
	return opts
}

func (opts *IndexOptions) WithMaxActiveSnapshots(maxActiveSnapshots int) *IndexOptions {
	opts.MaxActiveSnapshots = maxActiveSnapshots
	return opts
}

func (opts *IndexOptions) WithRenewSnapRootAfter(renewSnapRootAfter time.Duration) *IndexOptions {
	opts.RenewSnapRootAfter = renewSnapRootAfter
	return opts
}

func (opts *IndexOptions) WithCompactionThld(compactionThld float64) *IndexOptions {
	opts.CompactionThld = compactionThld
	return opts
}

func (opts *IndexOptions) WithDelayDuringCompaction(delayDuringCompaction time.Duration) *IndexOptions {
	opts.DelayDuringCompaction = delayDuringCompaction
	return opts
}

func (opts *IndexOptions) WithNodesLogMaxOpenedFiles(nodesLogMaxOpenedFiles int) *IndexOptions {
	opts.NodesLogMaxOpenedFiles = nodesLogMaxOpenedFiles
	return opts
}

func (opts *IndexOptions) WithHistoryLogMaxOpenedFiles(historyLogMaxOpenedFiles int) *IndexOptions {
	opts.HistoryLogMaxOpenedFiles = historyLogMaxOpenedFiles
	return opts
}

func (opts *IndexOptions) WithNumIndexers(n int) *IndexOptions {
	opts.NumIndexers = n
	return opts
}

func (opts *IndexOptions) WithSharedWriteBufferSize(size int) *IndexOptions {
	opts.SharedWriteBufferSize = size
	return opts
}

func (opts *IndexOptions) WithWriteBufferChunkSize(size int) *IndexOptions {
	opts.WriteBufferChunkSize = size
	return opts
}

func (opts *IndexOptions) WithMinWriteBufferSize(size int) *IndexOptions {
	opts.MinWriteBufferSize = size
	return opts
}

func (opts *IndexOptions) WithMaxWriteBufferSize(size int) *IndexOptions {
	opts.MaxWriteBufferSize = size
	return opts
}

func (opts *IndexOptions) WithPageBufferSize(size int) *IndexOptions {
	opts.PageBufferSize = size
	return opts
}

func (opts *IndexOptions) WithBackPressureMinDelay(delay time.Duration) *IndexOptions {
	opts.BackpressureMinDelay = delay
	return opts
}

func (opts *IndexOptions) WithBackPressureMaxDelay(delay time.Duration) *IndexOptions {
	opts.BackpressureMaxDelay = delay
	return opts
}
