/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/codenotary/immudb/embedded/ahtree"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/replication"
)

type Milliseconds int64

type dbOptions struct {
	Database string `json:"database"`

	synced        bool         // currently a global immudb instance option
	SyncFrequency Milliseconds `json:"syncFrequency"` // ms

	// replication options (field names must be kept for backwards compatibility)
	Replica                      bool   `json:"replica"`
	SyncReplication              bool   `json:"syncReplication"`
	PrimaryDatabase              string `json:"masterDatabase"`
	PrimaryHost                  string `json:"masterAddress"`
	PrimaryPort                  int    `json:"masterPort"`
	PrimaryUsername              string `json:"followerUsername"`
	PrimaryPassword              string `json:"followerPassword"`
	SyncAcks                     int    `json:"syncAcks"`
	PrefetchTxBufferSize         int    `json:"prefetchTxBufferSize"`
	ReplicationCommitConcurrency int    `json:"replicationCommitConcurrency"`
	AllowTxDiscarding            bool   `json:"allowTxDiscarding"`
	SkipIntegrityCheck           bool   `json:"skipIntegrityCheck"`
	WaitForIndexing              bool   `json:"waitForIndexing"`

	// store options
	EmbeddedValues bool `json:"embeddedValues"` // permanent
	PreallocFiles  bool `json:"preallocFiles"`  // permanent
	FileSize       int  `json:"fileSize"`       // permanent
	MaxKeyLen      int  `json:"maxKeyLen"`      // permanent
	MaxValueLen    int  `json:"maxValueLen"`    // permanent
	MaxTxEntries   int  `json:"maxTxEntries"`   // permanent

	ExcludeCommitTime bool `json:"excludeCommitTime"`

	MaxActiveTransactions int `json:"maxActiveTransactions"`
	MVCCReadSetLimit      int `json:"mvccReadSetLimit"`

	MaxConcurrency   int `json:"maxConcurrency"`
	MaxIOConcurrency int `json:"maxIOConcurrency"`

	WriteBufferSize int `json:"writeBufferSize"`

	TxLogCacheSize          int `json:"txLogCacheSize"`
	VLogCacheSize           int `json:"vLogCacheSize"`
	VLogMaxOpenedFiles      int `json:"vLogMaxOpenedFiles"`
	TxLogMaxOpenedFiles     int `json:"txLogMaxOpenedFiles"`
	CommitLogMaxOpenedFiles int `json:"commitLogMaxOpenedFiles"`
	WriteTxHeaderVersion    int `json:"writeTxHeaderVersion"`

	ReadTxPoolSize int `json:"readTxPoolSize"`

	IndexOptions *indexOptions `json:"indexOptions"`

	AHTOptions *ahtOptions `json:"ahtOptions"`

	Autoload featureState `json:"autoload"` // unspecfied is considered as enabled for backward compatibility

	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedBy string    `json:"updatedBy"`
	UpdatedAt time.Time `json:"updatedAt"`

	RetentionPeriod     Milliseconds `json:"retentionPeriod"`
	TruncationFrequency Milliseconds `json:"truncationFrequency"` // ms
}

type featureState int

const (
	unspecifiedState featureState = 0
	enabledState     featureState = 1
	disabledState    featureState = 2
)

func (fs featureState) isEnabled() bool {
	return fs == unspecifiedState || fs == enabledState
}

type indexOptions struct {
	FlushThreshold           int          `json:"flushThreshold"`
	SyncThreshold            int          `json:"syncThreshold"`
	FlushBufferSize          int          `json:"flushBufferSize"`
	CleanupPercentage        float32      `json:"cleanupPercentage"`
	CacheSize                int          `json:"cacheSize"`
	MaxNodeSize              int          `json:"maxNodeSize"` // permanent
	MaxActiveSnapshots       int          `json:"maxActiveSnapshots"`
	RenewSnapRootAfter       int64        `json:"renewSnapRootAfter"` // ms
	CompactionThld           int          `json:"compactionThld"`
	DelayDuringCompaction    int64        `json:"delayDuringCompaction"` // ms
	NodesLogMaxOpenedFiles   int          `json:"nodesLogMaxOpenedFiles"`
	HistoryLogMaxOpenedFiles int          `json:"historyLogMaxOpenedFiles"`
	CommitLogMaxOpenedFiles  int          `json:"commitLogMaxOpenedFiles"`
	MaxBulkSize              int          `json:"maxBulkSize"`
	BulkPreparationTimeout   Milliseconds `json:"bulkPreparationTimeout"` // ms
}

type ahtOptions struct {
	SyncThreshold   int `json:"syncThreshold"`
	WriteBufferSize int `json:"writeBufferSize"`
}

const (
	DefaultMaxValueLen   = 1 << 25 //32Mb
	DefaultStoreFileSize = 1 << 29 //512Mb
)

func (s *ImmuServer) defaultDBOptions(dbName, userName string) *dbOptions {
	dbOpts := &dbOptions{
		Database: dbName,

		synced:        s.Options.synced,
		SyncFrequency: Milliseconds(store.DefaultSyncFrequency.Milliseconds()),

		EmbeddedValues: store.DefaultEmbeddedValues,
		PreallocFiles:  store.DefaultPreallocFiles,
		FileSize:       DefaultStoreFileSize,
		MaxKeyLen:      store.DefaultMaxKeyLen,
		MaxValueLen:    DefaultMaxValueLen,
		MaxTxEntries:   store.DefaultMaxTxEntries,

		ExcludeCommitTime: false,

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
		ReadTxPoolSize:          database.DefaultReadTxPoolSize,

		IndexOptions: s.defaultIndexOptions(),

		AHTOptions: s.defaultAHTOptions(),

		Autoload: unspecifiedState,

		CreatedAt:           time.Now(),
		CreatedBy:           userName,
		TruncationFrequency: Milliseconds(database.DefaultTruncationFrequency.Milliseconds()),
	}

	if dbName == s.Options.systemAdminDBName || dbName == s.Options.defaultDBName {
		repOpts := s.Options.ReplicationOptions

		dbOpts.Replica = repOpts != nil && repOpts.IsReplica

		dbOpts.SyncReplication = repOpts.SyncReplication

		if dbOpts.Replica {
			dbOpts.PrimaryDatabase = dbOpts.Database // replica of systemdb and defaultdb must have the same name as in primary
			dbOpts.PrimaryHost = repOpts.PrimaryHost
			dbOpts.PrimaryPort = repOpts.PrimaryPort
			dbOpts.PrimaryUsername = repOpts.PrimaryUsername
			dbOpts.PrimaryPassword = repOpts.PrimaryPassword
			dbOpts.PrefetchTxBufferSize = repOpts.PrefetchTxBufferSize
			dbOpts.ReplicationCommitConcurrency = repOpts.ReplicationCommitConcurrency
			dbOpts.AllowTxDiscarding = repOpts.AllowTxDiscarding
			dbOpts.SkipIntegrityCheck = repOpts.SkipIntegrityCheck
		} else {
			dbOpts.SyncAcks = repOpts.SyncAcks
		}
	}

	return dbOpts
}

func (s *ImmuServer) defaultIndexOptions() *indexOptions {
	return &indexOptions{
		FlushThreshold:           tbtree.DefaultFlushThld,
		SyncThreshold:            tbtree.DefaultSyncThld,
		FlushBufferSize:          tbtree.DefaultFlushBufferSize,
		CleanupPercentage:        tbtree.DefaultCleanUpPercentage,
		CacheSize:                tbtree.DefaultCacheSize,
		MaxNodeSize:              tbtree.DefaultMaxNodeSize,
		MaxActiveSnapshots:       tbtree.DefaultMaxActiveSnapshots,
		RenewSnapRootAfter:       tbtree.DefaultRenewSnapRootAfter.Milliseconds(),
		CompactionThld:           tbtree.DefaultCompactionThld,
		DelayDuringCompaction:    tbtree.DefaultDelayDuringCompaction.Milliseconds(),
		NodesLogMaxOpenedFiles:   tbtree.DefaultNodesLogMaxOpenedFiles,
		HistoryLogMaxOpenedFiles: tbtree.DefaultHistoryLogMaxOpenedFiles,
		CommitLogMaxOpenedFiles:  tbtree.DefaultCommitLogMaxOpenedFiles,
		MaxBulkSize:              store.DefaultIndexingMaxBulkSize,
		BulkPreparationTimeout:   Milliseconds(store.DefaultBulkPreparationTimeout.Milliseconds()),
	}
}

func (s *ImmuServer) defaultAHTOptions() *ahtOptions {
	return &ahtOptions{
		SyncThreshold:   ahtree.DefaultSyncThld,
		WriteBufferSize: ahtree.DefaultWriteBufferSize,
	}
}

func (s *ImmuServer) databaseOptionsFrom(opts *dbOptions) *database.Options {
	return database.DefaultOptions().
		WithDBRootPath(s.Options.Dir).
		WithStoreOptions(s.storeOptionsForDB(opts.Database, s.remoteStorage, opts.storeOptions())).
		AsReplica(opts.Replica).
		WithSyncReplication(opts.SyncReplication).
		WithSyncAcks(opts.SyncAcks).
		WithReadTxPoolSize(opts.ReadTxPoolSize).
		WithRetentionPeriod(time.Millisecond * time.Duration(opts.RetentionPeriod)).
		WithTruncationFrequency(time.Millisecond * time.Duration(opts.TruncationFrequency)).
		WithMaxResultSize(s.Options.MaxResultSize)
}

func (opts *dbOptions) storeOptions() *store.Options {
	indexOpts := store.DefaultIndexOptions()

	if opts.IndexOptions != nil {
		indexOpts.
			WithFlushThld(opts.IndexOptions.FlushThreshold).
			WithSyncThld(opts.IndexOptions.SyncThreshold).
			WithFlushBufferSize(opts.IndexOptions.FlushBufferSize).
			WithCleanupPercentage(opts.IndexOptions.CleanupPercentage).
			WithCacheSize(opts.IndexOptions.CacheSize).
			WithMaxNodeSize(opts.IndexOptions.MaxNodeSize).
			WithMaxActiveSnapshots(opts.IndexOptions.MaxActiveSnapshots).
			WithRenewSnapRootAfter(time.Millisecond * time.Duration(opts.IndexOptions.RenewSnapRootAfter)).
			WithCompactionThld(opts.IndexOptions.CompactionThld).
			WithDelayDuringCompaction(time.Millisecond * time.Duration(opts.IndexOptions.DelayDuringCompaction)).
			WithNodesLogMaxOpenedFiles(opts.IndexOptions.NodesLogMaxOpenedFiles).
			WithHistoryLogMaxOpenedFiles(opts.IndexOptions.HistoryLogMaxOpenedFiles).
			WithCommitLogMaxOpenedFiles(opts.IndexOptions.CommitLogMaxOpenedFiles).
			WithMaxBulkSize(opts.IndexOptions.MaxBulkSize).
			WithBulkPreparationTimeout(time.Millisecond * time.Duration(opts.IndexOptions.BulkPreparationTimeout))
	}

	ahtOpts := store.DefaultAHTOptions()

	if opts.AHTOptions != nil {
		ahtOpts.WithSyncThld(opts.AHTOptions.SyncThreshold)
		ahtOpts.WithWriteBufferSize(opts.AHTOptions.WriteBufferSize)
	}

	stOpts := store.DefaultOptions().
		WithEmbeddedValues(opts.EmbeddedValues).
		WithPreallocFiles(opts.PreallocFiles).
		WithSynced(opts.synced).
		WithSyncFrequency(time.Millisecond * time.Duration(opts.SyncFrequency)).
		WithFileSize(opts.FileSize).
		WithMaxKeyLen(opts.MaxKeyLen).
		WithMaxValueLen(opts.MaxValueLen).
		WithMaxTxEntries(opts.MaxTxEntries).
		WithWriteTxHeaderVersion(opts.WriteTxHeaderVersion).
		WithMaxActiveTransactions(opts.MaxActiveTransactions).
		WithMVCCReadSetLimit(opts.MVCCReadSetLimit).
		WithMaxConcurrency(opts.MaxConcurrency).
		WithMaxIOConcurrency(opts.MaxIOConcurrency).
		WithWriteBufferSize(opts.WriteBufferSize).
		WithTxLogCacheSize(opts.TxLogCacheSize).
		WithVLogCacheSize(opts.VLogCacheSize).
		WithVLogMaxOpenedFiles(opts.VLogMaxOpenedFiles).
		WithTxLogMaxOpenedFiles(opts.TxLogMaxOpenedFiles).
		WithCommitLogMaxOpenedFiles(opts.CommitLogMaxOpenedFiles).
		WithIndexOptions(indexOpts).
		WithAHTOptions(ahtOpts)

	if opts.ExcludeCommitTime {
		stOpts.WithTimeFunc(func() time.Time { return time.Unix(0, 0) })
	} else {
		stOpts.WithTimeFunc(func() time.Time { return time.Now() })
	}

	return stOpts
}

func (opts *dbOptions) databaseNullableSettings() *schema.DatabaseNullableSettings {
	return &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:                      &schema.NullableBool{Value: opts.Replica},
			SyncReplication:              &schema.NullableBool{Value: opts.SyncReplication},
			PrimaryDatabase:              &schema.NullableString{Value: opts.PrimaryDatabase},
			PrimaryHost:                  &schema.NullableString{Value: opts.PrimaryHost},
			PrimaryPort:                  &schema.NullableUint32{Value: uint32(opts.PrimaryPort)},
			PrimaryUsername:              &schema.NullableString{Value: opts.PrimaryUsername},
			PrimaryPassword:              &schema.NullableString{Value: opts.PrimaryPassword},
			SyncAcks:                     &schema.NullableUint32{Value: uint32(opts.SyncAcks)},
			PrefetchTxBufferSize:         &schema.NullableUint32{Value: uint32(opts.PrefetchTxBufferSize)},
			ReplicationCommitConcurrency: &schema.NullableUint32{Value: uint32(opts.ReplicationCommitConcurrency)},
			AllowTxDiscarding:            &schema.NullableBool{Value: opts.AllowTxDiscarding},
			SkipIntegrityCheck:           &schema.NullableBool{Value: opts.SkipIntegrityCheck},
			WaitForIndexing:              &schema.NullableBool{Value: opts.WaitForIndexing},
		},

		SyncFrequency: &schema.NullableMilliseconds{Value: int64(opts.SyncFrequency)},

		FileSize:       &schema.NullableUint32{Value: uint32(opts.FileSize)},
		MaxKeyLen:      &schema.NullableUint32{Value: uint32(opts.MaxKeyLen)},
		MaxValueLen:    &schema.NullableUint32{Value: uint32(opts.MaxValueLen)},
		MaxTxEntries:   &schema.NullableUint32{Value: uint32(opts.MaxTxEntries)},
		EmbeddedValues: &schema.NullableBool{Value: opts.EmbeddedValues},
		PreallocFiles:  &schema.NullableBool{Value: opts.PreallocFiles},

		ExcludeCommitTime: &schema.NullableBool{Value: opts.ExcludeCommitTime},

		MaxActiveTransactions: &schema.NullableUint32{Value: uint32(opts.MaxActiveTransactions)},
		MvccReadSetLimit:      &schema.NullableUint32{Value: uint32(opts.MVCCReadSetLimit)},

		MaxConcurrency:   &schema.NullableUint32{Value: uint32(opts.MaxConcurrency)},
		MaxIOConcurrency: &schema.NullableUint32{Value: uint32(opts.MaxIOConcurrency)},

		WriteBufferSize: &schema.NullableUint32{Value: uint32(opts.WriteBufferSize)},

		TxLogCacheSize:          &schema.NullableUint32{Value: uint32(opts.TxLogCacheSize)},
		VLogCacheSize:           &schema.NullableUint32{Value: uint32(opts.VLogCacheSize)},
		VLogMaxOpenedFiles:      &schema.NullableUint32{Value: uint32(opts.VLogMaxOpenedFiles)},
		TxLogMaxOpenedFiles:     &schema.NullableUint32{Value: uint32(opts.TxLogMaxOpenedFiles)},
		CommitLogMaxOpenedFiles: &schema.NullableUint32{Value: uint32(opts.CommitLogMaxOpenedFiles)},

		IndexSettings: &schema.IndexNullableSettings{
			FlushThreshold:           &schema.NullableUint32{Value: uint32(opts.IndexOptions.FlushThreshold)},
			SyncThreshold:            &schema.NullableUint32{Value: uint32(opts.IndexOptions.SyncThreshold)},
			FlushBufferSize:          &schema.NullableUint32{Value: uint32(opts.IndexOptions.FlushBufferSize)},
			CleanupPercentage:        &schema.NullableFloat{Value: opts.IndexOptions.CleanupPercentage},
			CacheSize:                &schema.NullableUint32{Value: uint32(opts.IndexOptions.CacheSize)},
			MaxNodeSize:              &schema.NullableUint32{Value: uint32(opts.IndexOptions.MaxNodeSize)},
			MaxActiveSnapshots:       &schema.NullableUint32{Value: uint32(opts.IndexOptions.MaxActiveSnapshots)},
			RenewSnapRootAfter:       &schema.NullableUint64{Value: uint64(opts.IndexOptions.RenewSnapRootAfter)},
			CompactionThld:           &schema.NullableUint32{Value: uint32(opts.IndexOptions.CompactionThld)},
			DelayDuringCompaction:    &schema.NullableUint32{Value: uint32(opts.IndexOptions.DelayDuringCompaction)},
			NodesLogMaxOpenedFiles:   &schema.NullableUint32{Value: uint32(opts.IndexOptions.NodesLogMaxOpenedFiles)},
			HistoryLogMaxOpenedFiles: &schema.NullableUint32{Value: uint32(opts.IndexOptions.HistoryLogMaxOpenedFiles)},
			CommitLogMaxOpenedFiles:  &schema.NullableUint32{Value: uint32(opts.IndexOptions.CommitLogMaxOpenedFiles)},
			MaxBulkSize:              &schema.NullableUint32{Value: uint32(opts.IndexOptions.MaxBulkSize)},
			BulkPreparationTimeout:   &schema.NullableMilliseconds{Value: int64(opts.IndexOptions.BulkPreparationTimeout)},
		},

		AhtSettings: &schema.AHTNullableSettings{
			SyncThreshold:   &schema.NullableUint32{Value: uint32(opts.AHTOptions.SyncThreshold)},
			WriteBufferSize: &schema.NullableUint32{Value: uint32(opts.AHTOptions.WriteBufferSize)},
		},

		WriteTxHeaderVersion: &schema.NullableUint32{Value: uint32(opts.WriteTxHeaderVersion)},

		Autoload: &schema.NullableBool{Value: opts.Autoload.isEnabled()},

		ReadTxPoolSize: &schema.NullableUint32{Value: uint32(opts.ReadTxPoolSize)},

		TruncationSettings: &schema.TruncationNullableSettings{
			RetentionPeriod:     &schema.NullableMilliseconds{Value: int64(opts.RetentionPeriod)},
			TruncationFrequency: &schema.NullableMilliseconds{Value: int64(opts.TruncationFrequency)},
		},
	}
}

// dbSettingsToDbSettingsV2 converts old schema.DatabaseSettings message into new schema.DatabaseSettingsV2
// This is to add compatibility between old API using DatabaseSettings with new ones.
// Only those fields that were present up to the 1.2.2 release are supported.
// Changing any other fields requires new API calls.
func dbSettingsToDBNullableSettings(settings *schema.DatabaseSettings) *schema.DatabaseNullableSettings {
	nullableUInt32 := func(v uint32) *schema.NullableUint32 {
		if v > 0 {
			return &schema.NullableUint32{
				Value: v,
			}
		}
		return nil
	}

	repSettings := &schema.ReplicationNullableSettings{
		Replica:         &schema.NullableBool{Value: settings.Replica},
		PrimaryDatabase: &schema.NullableString{Value: settings.PrimaryDatabase},
		PrimaryHost:     &schema.NullableString{Value: settings.PrimaryHost},
		PrimaryPort:     &schema.NullableUint32{Value: settings.PrimaryPort},
		PrimaryUsername: &schema.NullableString{Value: settings.PrimaryUsername},
		PrimaryPassword: &schema.NullableString{Value: settings.PrimaryPassword},
	}

	if !settings.Replica {
		repSettings.SyncAcks = &schema.NullableUint32{}
		repSettings.PrefetchTxBufferSize = &schema.NullableUint32{}
		repSettings.ReplicationCommitConcurrency = &schema.NullableUint32{}
	}

	ret := &schema.DatabaseNullableSettings{
		ReplicationSettings: repSettings,
		FileSize:            nullableUInt32(settings.FileSize),
		MaxKeyLen:           nullableUInt32(settings.MaxKeyLen),
		MaxValueLen:         nullableUInt32(settings.MaxValueLen),
		MaxTxEntries:        nullableUInt32(settings.MaxTxEntries),
	}

	return ret
}

func (s *ImmuServer) overwriteWith(opts *dbOptions, settings *schema.DatabaseNullableSettings, existentDB bool) error {
	if existentDB {
		// permanent settings can not be changed after database is created
		// in the future, some settings may turn into non-permanent

		if settings.FileSize != nil {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments, "file size", opts.Database)
		}

		if settings.MaxKeyLen != nil {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments, "max key length", opts.Database)
		}

		if settings.MaxValueLen != nil {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments, "max value length", opts.Database)
		}

		if settings.MaxTxEntries != nil {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments,
				"max number of entries per transaction", opts.Database)
		}

		if settings.EmbeddedValues != nil {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments,
				"embedded values", opts.Database)
		}

		if settings.PreallocFiles != nil {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments,
				"prealloc files", opts.Database)
		}

		if settings.IndexSettings != nil && settings.IndexSettings.MaxNodeSize != nil {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments, "max node size", opts.Database)
		}

		opts.UpdatedAt = time.Now()
	}

	opts.synced = s.Options.synced

	// database instance options
	if settings.ReadTxPoolSize != nil {
		opts.ReadTxPoolSize = int(settings.ReadTxPoolSize.Value)
	}

	// replication settings
	if settings.ReplicationSettings != nil {
		rs := settings.ReplicationSettings

		if rs.Replica != nil {
			opts.Replica = rs.Replica.Value
		}
		if rs.SyncReplication != nil {
			opts.SyncReplication = rs.SyncReplication.Value
		}
		if rs.SyncAcks != nil {
			opts.SyncAcks = int(rs.SyncAcks.Value)
		} else if opts.Replica {
			opts.SyncAcks = 0
		}
		if rs.PrimaryDatabase != nil {
			opts.PrimaryDatabase = rs.PrimaryDatabase.Value
		} else if !opts.Replica {
			opts.PrimaryDatabase = ""
		}
		if rs.PrimaryHost != nil {
			opts.PrimaryHost = rs.PrimaryHost.Value
		} else if !opts.Replica {
			opts.PrimaryHost = ""
		}
		if rs.PrimaryPort != nil {
			opts.PrimaryPort = int(rs.PrimaryPort.Value)
		} else if !opts.Replica {
			opts.PrimaryPort = 0
		}
		if rs.PrimaryUsername != nil {
			opts.PrimaryUsername = rs.PrimaryUsername.Value
		} else if !opts.Replica {
			opts.PrimaryUsername = ""
		}
		if rs.PrimaryPassword != nil {
			opts.PrimaryPassword = rs.PrimaryPassword.Value
		} else if !opts.Replica {
			opts.PrimaryPassword = ""
		}
		if rs.PrefetchTxBufferSize != nil {
			opts.PrefetchTxBufferSize = int(rs.PrefetchTxBufferSize.Value)
		} else if opts.Replica && opts.PrefetchTxBufferSize == 0 {
			// set default value when it's not set
			opts.PrefetchTxBufferSize = replication.DefaultPrefetchTxBufferSize
		} else if !opts.Replica {
			opts.PrefetchTxBufferSize = 0
		}
		if rs.ReplicationCommitConcurrency != nil {
			opts.ReplicationCommitConcurrency = int(rs.ReplicationCommitConcurrency.Value)
		} else if opts.Replica && opts.ReplicationCommitConcurrency == 0 {
			// set default value when it's not set
			opts.ReplicationCommitConcurrency = replication.DefaultReplicationCommitConcurrency
		} else if !opts.Replica {
			opts.ReplicationCommitConcurrency = 0
		}
		if rs.AllowTxDiscarding != nil {
			opts.AllowTxDiscarding = rs.AllowTxDiscarding.Value
		}
		if rs.SkipIntegrityCheck != nil {
			opts.SkipIntegrityCheck = rs.SkipIntegrityCheck.Value
		}
		if rs.WaitForIndexing != nil {
			opts.WaitForIndexing = rs.WaitForIndexing.Value
		}
	}

	// store options

	if settings.SyncFrequency != nil {
		opts.SyncFrequency = Milliseconds(settings.SyncFrequency.Value)
	}

	if settings.FileSize != nil {
		opts.FileSize = int(settings.FileSize.Value)
	}

	if settings.MaxKeyLen != nil {
		opts.MaxKeyLen = int(settings.MaxKeyLen.Value)
	}

	if settings.MaxValueLen != nil {
		opts.MaxValueLen = int(settings.MaxValueLen.Value)
	}

	if settings.MaxTxEntries != nil {
		opts.MaxTxEntries = int(settings.MaxTxEntries.Value)
	}

	if settings.EmbeddedValues != nil {
		opts.EmbeddedValues = settings.EmbeddedValues.Value
	}

	if settings.PreallocFiles != nil {
		opts.PreallocFiles = settings.PreallocFiles.Value
	}

	if settings.ExcludeCommitTime != nil {
		opts.ExcludeCommitTime = settings.ExcludeCommitTime.Value
	}

	if settings.MaxActiveTransactions != nil {
		opts.MaxActiveTransactions = int(settings.MaxActiveTransactions.Value)
	}
	if settings.MvccReadSetLimit != nil {
		opts.MVCCReadSetLimit = int(settings.MvccReadSetLimit.Value)
	}

	if settings.MaxConcurrency != nil {
		opts.MaxConcurrency = int(settings.MaxConcurrency.Value)
	}
	if settings.MaxIOConcurrency != nil {
		opts.MaxIOConcurrency = int(settings.MaxIOConcurrency.Value)
	}

	if settings.WriteBufferSize != nil {
		opts.WriteBufferSize = int(settings.WriteBufferSize.Value)
	}

	if settings.TxLogCacheSize != nil {
		opts.TxLogCacheSize = int(settings.TxLogCacheSize.Value)
	}
	if settings.VLogCacheSize != nil {
		opts.VLogCacheSize = int(settings.VLogCacheSize.Value)
	}
	if settings.VLogMaxOpenedFiles != nil {
		opts.VLogMaxOpenedFiles = int(settings.VLogMaxOpenedFiles.Value)
	}
	if settings.TxLogMaxOpenedFiles != nil {
		opts.TxLogMaxOpenedFiles = int(settings.TxLogMaxOpenedFiles.Value)
	}
	if settings.CommitLogMaxOpenedFiles != nil {
		opts.CommitLogMaxOpenedFiles = int(settings.CommitLogMaxOpenedFiles.Value)
	}

	if settings.WriteTxHeaderVersion != nil {
		opts.WriteTxHeaderVersion = int(settings.WriteTxHeaderVersion.Value)
	}

	if settings.Autoload != nil {
		if settings.Autoload.Value {
			opts.Autoload = enabledState
		} else {
			opts.Autoload = disabledState
		}
	}

	if settings.TruncationSettings != nil {
		if settings.TruncationSettings.RetentionPeriod != nil {
			opts.RetentionPeriod = Milliseconds(settings.TruncationSettings.RetentionPeriod.Value)
		}

		if settings.TruncationSettings.TruncationFrequency != nil {
			opts.TruncationFrequency = Milliseconds(settings.TruncationSettings.TruncationFrequency.Value)
		}
	}

	// index options
	if settings.IndexSettings != nil {
		if opts.IndexOptions == nil {
			opts.IndexOptions = s.defaultIndexOptions()
		}

		if settings.IndexSettings.FlushThreshold != nil {
			opts.IndexOptions.FlushThreshold = int(settings.IndexSettings.FlushThreshold.Value)
		}
		if settings.IndexSettings.SyncThreshold != nil {
			opts.IndexOptions.SyncThreshold = int(settings.IndexSettings.SyncThreshold.Value)
		}
		if settings.IndexSettings.FlushBufferSize != nil {
			opts.IndexOptions.FlushBufferSize = int(settings.IndexSettings.FlushBufferSize.Value)
		}
		if settings.IndexSettings.CleanupPercentage != nil {
			opts.IndexOptions.CleanupPercentage = settings.IndexSettings.CleanupPercentage.Value
		}
		if settings.IndexSettings.CacheSize != nil {
			opts.IndexOptions.CacheSize = int(settings.IndexSettings.CacheSize.Value)
		}
		if settings.IndexSettings.MaxNodeSize != nil {
			opts.IndexOptions.MaxNodeSize = int(settings.IndexSettings.MaxNodeSize.Value)
		}
		if settings.IndexSettings.MaxActiveSnapshots != nil {
			opts.IndexOptions.MaxActiveSnapshots = int(settings.IndexSettings.MaxActiveSnapshots.Value)
		}
		if settings.IndexSettings.RenewSnapRootAfter != nil {
			opts.IndexOptions.RenewSnapRootAfter = int64(settings.IndexSettings.RenewSnapRootAfter.Value)
		}
		if settings.IndexSettings.CompactionThld != nil {
			opts.IndexOptions.CompactionThld = int(settings.IndexSettings.CompactionThld.Value)
		}
		if settings.IndexSettings.DelayDuringCompaction != nil {
			opts.IndexOptions.DelayDuringCompaction = int64(settings.IndexSettings.DelayDuringCompaction.Value)
		}
		if settings.IndexSettings.NodesLogMaxOpenedFiles != nil {
			opts.IndexOptions.NodesLogMaxOpenedFiles = int(settings.IndexSettings.NodesLogMaxOpenedFiles.Value)
		}
		if settings.IndexSettings.HistoryLogMaxOpenedFiles != nil {
			opts.IndexOptions.HistoryLogMaxOpenedFiles = int(settings.IndexSettings.HistoryLogMaxOpenedFiles.Value)
		}
		if settings.IndexSettings.CommitLogMaxOpenedFiles != nil {
			opts.IndexOptions.CommitLogMaxOpenedFiles = int(settings.IndexSettings.CommitLogMaxOpenedFiles.Value)
		}
		if settings.IndexSettings.MaxBulkSize != nil {
			opts.IndexOptions.MaxBulkSize = int(settings.IndexSettings.MaxBulkSize.Value)
		}
		if settings.IndexSettings.BulkPreparationTimeout != nil {
			opts.IndexOptions.BulkPreparationTimeout = Milliseconds(settings.IndexSettings.BulkPreparationTimeout.Value)
		}
	}

	// aht options
	if settings.AhtSettings != nil {
		if opts.AHTOptions == nil {
			opts.AHTOptions = s.defaultAHTOptions()
		}

		if settings.AhtSettings.SyncThreshold != nil {
			opts.AHTOptions.SyncThreshold = int(settings.AhtSettings.SyncThreshold.Value)
		}

		if settings.AhtSettings.WriteBufferSize != nil {
			opts.AHTOptions.WriteBufferSize = int(settings.AhtSettings.WriteBufferSize.Value)
		}
	}

	err := opts.Validate()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrIllegalArguments, err)
	}

	return nil
}

func (opts *dbOptions) Validate() error {
	if opts.Replica {
		if opts.PrefetchTxBufferSize <= 0 {
			return fmt.Errorf(
				"%w: invalid value for replication option PrefetchTxBufferSize on replica database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.ReplicationCommitConcurrency <= 0 {
			return fmt.Errorf(
				"%w: invalid value for replication option ReplicationCommitConcurrency on replica database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.SyncAcks > 0 {
			return fmt.Errorf(
				"%w: invalid value for replication option SyncAcks ReplicationCommitConcurrency on database '%s'",
				ErrIllegalArguments, opts.Database)
		}

	} else {
		if opts.SyncAcks < 0 {
			return fmt.Errorf(
				"%w: invalid value for replication option SyncAcks on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.PrimaryDatabase != "" {
			return fmt.Errorf(
				"%w: invalid value for replication option PrimaryDatabase on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.PrimaryHost != "" {
			return fmt.Errorf(
				"%w: invalid value for replication option PrimaryHost on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.PrimaryPort > 0 {
			return fmt.Errorf(
				"%w: invalid value for replication option PrimaryPort on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.PrimaryUsername != "" {
			return fmt.Errorf(
				"%w: invalid value for replication option PrimaryUsername on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.PrimaryPassword != "" {
			return fmt.Errorf(
				"%w: invalid value for replication option PrimaryPassword on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.PrefetchTxBufferSize > 0 {
			return fmt.Errorf(
				"%w: invalid value for replication option PrefetchTxBufferSize on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.ReplicationCommitConcurrency > 0 {
			return fmt.Errorf(
				"%w: invalid value for replication option ReplicationCommitConcurrency on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.AllowTxDiscarding {
			return fmt.Errorf(
				"%w: invalid value for replication option AllowTxDiscarding on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.SkipIntegrityCheck {
			return fmt.Errorf(
				"%w: invalid value for replication option SkipIntegrityCheck on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.WaitForIndexing {
			return fmt.Errorf(
				"%w: invalid value for replication option WaitForIndexing on primary database '%s'",
				ErrIllegalArguments, opts.Database)
		}

		if opts.SyncReplication && opts.SyncAcks == 0 {
			return fmt.Errorf(
				"%w: invalid replication options for primary database '%s'. It is necessary to have at least one sync replica",
				ErrIllegalArguments, opts.Database)
		}

		if !opts.SyncReplication && opts.SyncAcks > 0 {
			return fmt.Errorf(
				"%w: invalid replication options for primary database '%s'. SyncAcks should be set to 0",
				ErrIllegalArguments, opts.Database)
		}
	}

	if opts.ReadTxPoolSize <= 0 {
		return fmt.Errorf(
			"%w: invalid read tx pool size (%d) for database '%s'",
			ErrIllegalArguments, opts.ReadTxPoolSize, opts.Database,
		)
	}

	if opts.RetentionPeriod < 0 || (opts.RetentionPeriod > 0 && opts.RetentionPeriod < Milliseconds(store.MinimumRetentionPeriod.Milliseconds())) {
		return fmt.Errorf(
			"%w: invalid retention period for database '%s'. RetentionPeriod should at least '%v' hours",
			ErrIllegalArguments, opts.Database, store.MinimumRetentionPeriod.Hours())
	}

	if opts.TruncationFrequency < 0 || (opts.TruncationFrequency > 0 && opts.TruncationFrequency < Milliseconds(store.MinimumTruncationFrequency.Milliseconds())) {
		return fmt.Errorf(
			"%w: invalid truncation frequency for database '%s'. TruncationFrequency should at least '%v' hour",
			ErrIllegalArguments, opts.Database, store.MinimumTruncationFrequency.Hours())
	}

	return opts.storeOptions().Validate()
}

func (opts *dbOptions) isReplicatorRequired() bool {
	return opts.Replica &&
		opts.PrimaryDatabase != "" &&
		opts.PrimaryHost != "" &&
		opts.PrimaryPort > 0
}

func (opts *dbOptions) isDataRetentionEnabled() bool {
	return opts.RetentionPeriod > 0
}

func (s *ImmuServer) saveDBOptions(options *dbOptions) error {
	serializedOptions, err := json.Marshal(options)
	if err != nil {
		return err
	}

	optionsKey := make([]byte, 1+len(options.Database))
	optionsKey[0] = KeyPrefixDBSettings
	copy(optionsKey[1:], []byte(options.Database))

	_, err = s.sysDB.Set(context.Background(), &schema.SetRequest{KVs: []*schema.KeyValue{{Key: optionsKey, Value: serializedOptions}}})

	return err
}

func (s *ImmuServer) deleteDBOptionsFor(db string) error {
	optionsKey := make([]byte, 1+len(db))
	optionsKey[0] = KeyPrefixDBSettings
	copy(optionsKey[1:], []byte(db))

	_, err := s.sysDB.Delete(context.Background(), &schema.DeleteKeysRequest{
		Keys: [][]byte{
			optionsKey,
		},
	})

	return err
}

func (s *ImmuServer) loadDBOptions(database string, createIfNotExists bool) (*dbOptions, error) {
	if database == s.Options.systemAdminDBName || database == s.Options.defaultDBName {
		return s.defaultDBOptions(database, s.Options.systemAdminDBName), nil
	}

	optionsKey := make([]byte, 1+len(database))
	optionsKey[0] = KeyPrefixDBSettings
	copy(optionsKey[1:], []byte(database))

	options := s.defaultDBOptions(database, "")

	e, err := s.sysDB.Get(context.Background(), &schema.KeyRequest{Key: optionsKey})
	if errors.Is(err, store.ErrKeyNotFound) && createIfNotExists {
		err = s.saveDBOptions(options)
		if err != nil {
			return nil, err
		}
		return options, nil
	}
	if err != nil {
		return nil, err
	}

	err = json.Unmarshal(e.Value, &options)
	if err != nil {
		return nil, err
	}

	return options, nil
}

func (s *ImmuServer) logDBOptions(database string, opts *dbOptions) {
	// This list is manually updated to ensure we don't expose sensitive information
	// in logs such as replication passwords
	s.Logger.Infof("%s.Autoload: %v", database, opts.Autoload.isEnabled())
	s.Logger.Infof("%s.Synced: %v", database, opts.synced)
	s.Logger.Infof("%s.SyncFrequency: %v", database, opts.SyncFrequency)
	s.Logger.Infof("%s.Replica: %v", database, opts.Replica)
	s.Logger.Infof("%s.SyncReplication: %v", database, opts.SyncReplication)
	s.Logger.Infof("%s.SyncAcks: %v", database, opts.SyncAcks)
	s.Logger.Infof("%s.PrefetchTxBufferSize: %v", database, opts.PrefetchTxBufferSize)
	s.Logger.Infof("%s.ReplicationCommitConcurrency: %v", database, opts.ReplicationCommitConcurrency)
	s.Logger.Infof("%s.AllowTxDiscarding: %v", database, opts.AllowTxDiscarding)
	s.Logger.Infof("%s.SkipIntegrityCheck: %v", database, opts.SkipIntegrityCheck)
	s.Logger.Infof("%s.WaitForIndexing: %v", database, opts.WaitForIndexing)
	s.Logger.Infof("%s.FileSize: %v", database, opts.FileSize)
	s.Logger.Infof("%s.MaxKeyLen: %v", database, opts.MaxKeyLen)
	s.Logger.Infof("%s.MaxValueLen: %v", database, opts.MaxValueLen)
	s.Logger.Infof("%s.MaxTxEntries: %v", database, opts.MaxTxEntries)
	s.Logger.Infof("%s.EmbeddedValues: %v", database, opts.EmbeddedValues)
	s.Logger.Infof("%s.PreallocFiles: %v", database, opts.PreallocFiles)
	s.Logger.Infof("%s.ExcludeCommitTime: %v", database, opts.ExcludeCommitTime)
	s.Logger.Infof("%s.MaxActiveTransactions: %v", database, opts.MaxActiveTransactions)
	s.Logger.Infof("%s.MVCCReadSetLimit: %v", database, opts.MVCCReadSetLimit)
	s.Logger.Infof("%s.MaxConcurrency: %v", database, opts.MaxConcurrency)
	s.Logger.Infof("%s.MaxIOConcurrency: %v", database, opts.MaxIOConcurrency)
	s.Logger.Infof("%s.WriteBufferSize: %v", database, opts.WriteBufferSize)
	s.Logger.Infof("%s.TxLogCacheSize: %v", database, opts.TxLogCacheSize)
	s.Logger.Infof("%s.VLogCacheSize: %v", database, opts.VLogCacheSize)
	s.Logger.Infof("%s.VLogMaxOpenedFiles: %v", database, opts.VLogMaxOpenedFiles)
	s.Logger.Infof("%s.TxLogMaxOpenedFiles: %v", database, opts.TxLogMaxOpenedFiles)
	s.Logger.Infof("%s.CommitLogMaxOpenedFiles: %v", database, opts.CommitLogMaxOpenedFiles)
	s.Logger.Infof("%s.WriteTxHeaderVersion: %v", database, opts.WriteTxHeaderVersion)
	s.Logger.Infof("%s.ReadTxPoolSize: %v", database, opts.ReadTxPoolSize)
	s.Logger.Infof("%s.TruncationFrequency: %v", database, opts.TruncationFrequency)
	s.Logger.Infof("%s.RetentionPeriod: %v", database, opts.RetentionPeriod)
	s.Logger.Infof("%s.IndexOptions.FlushThreshold: %v", database, opts.IndexOptions.FlushThreshold)
	s.Logger.Infof("%s.IndexOptions.SyncThreshold: %v", database, opts.IndexOptions.SyncThreshold)
	s.Logger.Infof("%s.IndexOptions.FlushBufferSize: %v", database, opts.IndexOptions.FlushBufferSize)
	s.Logger.Infof("%s.IndexOptions.CleanupPercentage: %v", database, opts.IndexOptions.CleanupPercentage)
	s.Logger.Infof("%s.IndexOptions.CacheSize: %v", database, opts.IndexOptions.CacheSize)
	s.Logger.Infof("%s.IndexOptions.MaxNodeSize: %v", database, opts.IndexOptions.MaxNodeSize)
	s.Logger.Infof("%s.IndexOptions.MaxActiveSnapshots: %v", database, opts.IndexOptions.MaxActiveSnapshots)
	s.Logger.Infof("%s.IndexOptions.RenewSnapRootAfter: %v", database, opts.IndexOptions.RenewSnapRootAfter)
	s.Logger.Infof("%s.IndexOptions.CompactionThld: %v", database, opts.IndexOptions.CompactionThld)
	s.Logger.Infof("%s.IndexOptions.DelayDuringCompaction: %v", database, opts.IndexOptions.DelayDuringCompaction)
	s.Logger.Infof("%s.IndexOptions.NodesLogMaxOpenedFiles: %v", database, opts.IndexOptions.NodesLogMaxOpenedFiles)
	s.Logger.Infof("%s.IndexOptions.HistoryLogMaxOpenedFiles: %v", database, opts.IndexOptions.HistoryLogMaxOpenedFiles)
	s.Logger.Infof("%s.IndexOptions.CommitLogMaxOpenedFiles: %v", database, opts.IndexOptions.CommitLogMaxOpenedFiles)
	s.Logger.Infof("%s.IndexOptions.MaxBulkSize: %v", database, opts.IndexOptions.MaxBulkSize)
	s.Logger.Infof("%s.IndexOptions.BulkPreparationTimeout: %v", database, opts.IndexOptions.BulkPreparationTimeout)
	s.Logger.Infof("%s.AHTOptions.SyncThreshold: %v", database, opts.AHTOptions.SyncThreshold)
	s.Logger.Infof("%s.AHTOptions.WriteBufferSize: %v", database, opts.AHTOptions.WriteBufferSize)
}
