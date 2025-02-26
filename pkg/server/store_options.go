package server

import (
	"errors"

	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
)

var (
	ErrInvalidConfigOption = errors.New("invalid config option")
)

const (
	DefaultMaxValueLen   = 1 << 25 // 32Mb
	DefaultStoreFileSize = 1 << 29 // 512Mb
)

type StoreOptions struct {
	EmbeddedValues          bool          `json:"embeddedValues"` // permanent
	PreallocFiles           bool          `json:"preallocFiles"`  // permanent
	FileSize                int           `json:"fileSize"`       // permanent
	MaxKeyLen               int           `json:"maxKeyLen"`      // permanent
	MaxValueLen             int           `json:"maxValueLen"`    // permanent
	MaxTxEntries            int           `json:"maxTxEntries"`   // permanent
	ExcludeCommitTime       bool          `json:"excludeCommitTime"`
	MaxActiveTransactions   int           `json:"maxActiveTransactions"`
	MVCCReadSetLimit        int           `json:"mvccReadSetLimit"`
	MaxConcurrency          int           `json:"maxConcurrency"`
	MaxIOConcurrency        int           `json:"maxIOConcurrency"`
	WriteBufferSize         int           `json:"writeBufferSize"`
	TxLogCacheSize          int           `json:"txLogCacheSize"`
	VLogCacheSize           int           `json:"vLogCacheSize"`
	VLogMaxOpenedFiles      int           `json:"vLogMaxOpenedFiles"`
	TxLogMaxOpenedFiles     int           `json:"txLogMaxOpenedFiles"`
	CommitLogMaxOpenedFiles int           `json:"commitLogMaxOpenedFiles"`
	WriteTxHeaderVersion    int           `json:"writeTxHeaderVersion"`
	IndexOptions            *indexOptions `json:"indexOptions"`
	AHTOptions              *ahtOptions   `json:"ahtOptions"`
}

func defaultStoreOptions() StoreOptions {
	return StoreOptions{
		EmbeddedValues:          store.DefaultEmbeddedValues,
		PreallocFiles:           store.DefaultPreallocFiles,
		FileSize:                DefaultStoreFileSize,
		MaxKeyLen:               store.DefaultMaxKeyLen,
		MaxValueLen:             DefaultMaxValueLen,
		MaxTxEntries:            store.DefaultMaxTxEntries,
		ExcludeCommitTime:       false,
		MaxActiveTransactions:   store.DefaultMaxActiveTransactions,
		MVCCReadSetLimit:        store.DefaultMVCCReadSetLimit,
		MaxConcurrency:          store.DefaultMaxConcurrency,
		MaxIOConcurrency:        store.DefaultMaxIOConcurrency,
		WriteBufferSize:         store.DefaultWriteBufferSize,
		TxLogCacheSize:          store.DefaultTxLogCacheSize,
		VLogCacheSize:           store.DefaultVLogCacheSize,
		VLogMaxOpenedFiles:      store.DefaultVLogMaxOpenedFiles,
		TxLogMaxOpenedFiles:     store.DefaultTxLogMaxOpenedFiles,
		CommitLogMaxOpenedFiles: store.DefaultCommitLogMaxOpenedFiles,
		WriteTxHeaderVersion:    store.DefaultWriteTxHeaderVersion,
		IndexOptions:            defaultIndexOptions(),
		AHTOptions:              defaultAHTOptions(),
	}
}

func defaultIndexOptions() *indexOptions {
	return &indexOptions{
		//FlushThreshold:           tbtree.DefaultFlushThld,
		SyncThreshold:     tbtree.DefaultSyncThld,
		FlushBufferSize:   tbtree.DefaultFlushBufferSize,
		CleanupPercentage: tbtree.DefaultCleanUpPercentage,
		//CacheSize:                tbtree.DefaultCacheSize,
		//MaxNodeSize:              tbtree.DefaultMaxNodeSize,
		MaxActiveSnapshots: tbtree.DefaultMaxActiveSnapshots,
		RenewSnapRootAfter: tbtree.DefaultRenewSnapRootAfter.Milliseconds(),
		//CompactionThld:           tbtree.DefaultCompactionThld,
		//DelayDuringCompaction:    tbtree.DefaultDelayDuringCompaction.Milliseconds(),
		NodesLogMaxOpenedFiles:   tbtree.DefaultNodesLogMaxOpenedFiles,
		HistoryLogMaxOpenedFiles: tbtree.DefaultHistoryLogMaxOpenedFiles,
		CommitLogMaxOpenedFiles:  tbtree.DefaultCommitLogMaxOpenedFiles,
		MaxBulkSize:              store.DefaultIndexingMaxBulkSize,
		BulkPreparationTimeout:   Milliseconds(store.DefaultBulkPreparationTimeout.Milliseconds()),
	}
}

func defaultAHTOptions() *ahtOptions {
	return &ahtOptions{
		SyncThreshold:   ahtree.DefaultSyncThld,
		WriteBufferSize: ahtree.DefaultWriteBufferSize,
	}
}
