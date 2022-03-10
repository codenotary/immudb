/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package server

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/embedded/tbtree"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
)

type dbOptions struct {
	Database string `json:"database"`

	synced bool // currently a global immudb instance option

	// replication options
	Replica          bool   `json:"replica"`
	MasterDatabase   string `json:"masterDatabase"`
	MasterAddress    string `json:"masterAddress"`
	MasterPort       int    `json:"masterPort"`
	FollowerUsername string `json:"followerUsername"`
	FollowerPassword string `json:"followerPassword"`

	// store options
	FileSize     int `json:"fileSize"`     // permanent
	MaxKeyLen    int `json:"maxKeyLen"`    // permanent
	MaxValueLen  int `json:"maxValueLen"`  // permanent
	MaxTxEntries int `json:"maxTxEntries"` // permanent

	ExcludeCommitTime bool `json:"excludeCommitTime"`

	MaxConcurrency   int `json:"maxConcurrency"`
	MaxIOConcurrency int `json:"maxIOConcurrency"`

	TxLogCacheSize          int `json:"txLogCacheSize"`
	VLogMaxOpenedFiles      int `json:"vLogMaxOpenedFiles"`
	TxLogMaxOpenedFiles     int `json:"txLogMaxOpenedFiles"`
	CommitLogMaxOpenedFiles int `json:"commitLogMaxOpenedFiles"`

	IndexOptions *indexOptions `json:"indexOptions"`

	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedBy string    `json:"updatedBy"`
	UpdatedAt time.Time `json:"updatedAt"`
}

type indexOptions struct {
	FlushThreshold           int   `json:"flushThreshold"`
	SyncThreshold            int   `json:"syncThreshold"`
	FlushBufferSize          int   `json:"flushBufferSize"`
	CleanupPercentage        int   `json:"cleanupPercentage"`
	CacheSize                int   `json:"cacheSize"`
	MaxNodeSize              int   `json:"maxNodeSize"` // permanent
	MaxActiveSnapshots       int   `json:"maxActiveSnapshots"`
	RenewSnapRootAfter       int64 `json:"renewSnapRootAfter"` // ms
	CompactionThld           int   `json:"compactionThld"`
	DelayDuringCompaction    int64 `json:"delayDuringCompaction"` // ms
	NodesLogMaxOpenedFiles   int   `json:"nodesLogMaxOpenedFiles"`
	HistoryLogMaxOpenedFiles int   `json:"historyLogMaxOpenedFiles"`
	CommitLogMaxOpenedFiles  int   `json:"commitLogMaxOpenedFiles"`
}

const DefaultMaxValueLen = 1 << 25   //32Mb
const DefaultStoreFileSize = 1 << 29 //512Mb

func (s *ImmuServer) defaultDBOptions(database string) *dbOptions {
	return &dbOptions{
		Database: database,

		synced: s.Options.synced,

		Replica: s.Options.ReplicationOptions != nil,

		FileSize:     DefaultStoreFileSize,
		MaxKeyLen:    store.DefaultMaxKeyLen,
		MaxValueLen:  DefaultMaxValueLen,
		MaxTxEntries: store.DefaultMaxTxEntries,

		ExcludeCommitTime: false,

		MaxConcurrency:          store.DefaultMaxConcurrency,
		MaxIOConcurrency:        store.DefaultMaxIOConcurrency,
		TxLogCacheSize:          store.DefaultTxLogCacheSize,
		VLogMaxOpenedFiles:      store.DefaultVLogMaxOpenedFiles,
		TxLogMaxOpenedFiles:     store.DefaultTxLogMaxOpenedFiles,
		CommitLogMaxOpenedFiles: store.DefaultCommitLogMaxOpenedFiles,

		IndexOptions: s.defaultIndexOptions(),

		CreatedAt: time.Now(),
	}
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
	}
}

func (s *ImmuServer) databaseOptionsFrom(opts *dbOptions) *database.Options {
	return database.DefaultOption().
		WithDBRootPath(s.Options.Dir).
		WithStoreOptions(s.storeOptionsForDB(opts.Database, s.remoteStorage, opts.storeOptions())).
		AsReplica(opts.Replica)
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
			WithCommitLogMaxOpenedFiles(opts.IndexOptions.CommitLogMaxOpenedFiles)
	}

	stOpts := store.DefaultOptions().
		WithSynced(opts.synced).
		WithFileSize(opts.FileSize).
		WithMaxKeyLen(opts.MaxKeyLen).
		WithMaxValueLen(opts.MaxValueLen).
		WithMaxTxEntries(opts.MaxTxEntries).
		WithMaxConcurrency(opts.MaxConcurrency).
		WithMaxIOConcurrency(opts.MaxIOConcurrency).
		WithTxLogCacheSize(opts.TxLogCacheSize).
		WithVLogMaxOpenedFiles(opts.VLogMaxOpenedFiles).
		WithTxLogMaxOpenedFiles(opts.TxLogMaxOpenedFiles).
		WithCommitLogMaxOpenedFiles(opts.CommitLogMaxOpenedFiles).
		WithMaxLinearProofLen(0). // fixed no limitation, it may be customized in the future
		WithIndexOptions(indexOpts)

	if opts.ExcludeCommitTime {
		stOpts.WithTimeFunc(func() time.Time { return time.Unix(0, 0) })
	} else {
		stOpts.WithTimeFunc(func() time.Time { return time.Now() })
	}

	return stOpts
}

func (opts *dbOptions) databaseSettings() *schema.DatabaseSettings {
	return &schema.DatabaseSettings{
		DatabaseName: opts.Database,

		Replica:          opts.Replica,
		MasterDatabase:   opts.MasterDatabase,
		MasterAddress:    opts.MasterAddress,
		MasterPort:       uint32(opts.MasterPort),
		FollowerUsername: opts.FollowerUsername,
		FollowerPassword: opts.FollowerPassword,

		FileSize:     uint32(opts.FileSize),
		MaxKeyLen:    uint32(opts.MaxKeyLen),
		MaxValueLen:  uint32(opts.MaxValueLen),
		MaxTxEntries: uint32(opts.MaxTxEntries),

		ExcludeCommitTime: opts.ExcludeCommitTime,

		MaxConcurrency:   uint32(opts.MaxConcurrency),
		MaxIOConcurrency: uint32(opts.MaxIOConcurrency),

		TxLogCacheSize:          uint32(opts.TxLogCacheSize),
		VLogMaxOpenedFiles:      uint32(opts.VLogMaxOpenedFiles),
		TxLogMaxOpenedFiles:     uint32(opts.TxLogMaxOpenedFiles),
		CommitLogMaxOpenedFiles: uint32(opts.CommitLogMaxOpenedFiles),

		IndexSettings: &schema.IndexSettings{
			FlushThreshold:           uint32(opts.IndexOptions.FlushThreshold),
			SyncThreshold:            uint32(opts.IndexOptions.SyncThreshold),
			FlushBufferSize:          uint32(opts.IndexOptions.FlushBufferSize),
			CleanupPercentage:        uint32(opts.IndexOptions.CleanupPercentage),
			CacheSize:                uint32(opts.IndexOptions.CacheSize),
			MaxNodeSize:              uint32(opts.IndexOptions.MaxNodeSize),
			MaxActiveSnapshots:       uint32(opts.IndexOptions.MaxActiveSnapshots),
			RenewSnapRootAfter:       uint64(opts.IndexOptions.RenewSnapRootAfter),
			CompactionThld:           uint32(opts.IndexOptions.CompactionThld),
			DelayDuringCompaction:    uint32(opts.IndexOptions.DelayDuringCompaction),
			NodesLogMaxOpenedFiles:   uint32(opts.IndexOptions.NodesLogMaxOpenedFiles),
			HistoryLogMaxOpenedFiles: uint32(opts.IndexOptions.HistoryLogMaxOpenedFiles),
			CommitLogMaxOpenedFiles:  uint32(opts.IndexOptions.CommitLogMaxOpenedFiles),
		},
	}
}

var conditionalSet = func(condition bool, setter func()) {
	if condition {
		setter()
	}
}

func (s *ImmuServer) overwriteWith(opts *dbOptions, settings *schema.DatabaseSettings, existentDB bool) error {
	// replication options
	if !settings.Replica &&
		(settings.MasterDatabase != "" ||
			settings.MasterAddress != "" ||
			settings.MasterPort > 0 ||
			settings.FollowerUsername != "" ||
			settings.FollowerPassword != "") {
		return fmt.Errorf("%w: invalid replication options for database '%s'", ErrIllegalArguments, opts.Database)
	}

	if existentDB {
		// permanent settings can not be changed after database is created
		// in the future, some settings may turn into non-permanent

		if settings.FileSize > 0 {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments, "file size", opts.Database)
		}

		if settings.MaxKeyLen > 0 {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments, "max key lenght", opts.Database)
		}

		if settings.MaxValueLen > 0 {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments, "max value lenght", opts.Database)
		}

		if settings.MaxTxEntries > 0 {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments,
				"max number of entries per transaction", opts.Database)
		}

		if settings.IndexSettings != nil && settings.IndexSettings.MaxNodeSize > 0 {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments, "max node size", opts.Database)
		}

		opts.UpdatedAt = time.Now()
	}

	opts.synced = s.Options.synced

	opts.Replica = settings.Replica
	opts.MasterDatabase = settings.MasterDatabase
	opts.MasterAddress = settings.MasterAddress
	opts.MasterPort = int(settings.MasterPort)
	opts.FollowerUsername = settings.FollowerUsername
	opts.FollowerPassword = settings.FollowerPassword

	conditionalSet(settings.FileSize > 0, func() { opts.FileSize = int(settings.FileSize) })
	conditionalSet(settings.MaxKeyLen > 0, func() { opts.MaxKeyLen = int(settings.MaxKeyLen) })
	conditionalSet(settings.MaxValueLen > 0, func() { opts.MaxValueLen = int(settings.MaxValueLen) })
	conditionalSet(settings.MaxTxEntries > 0, func() { opts.MaxTxEntries = int(settings.MaxTxEntries) })

	// store options
	opts.ExcludeCommitTime = settings.ExcludeCommitTime

	conditionalSet(settings.MaxConcurrency > 0, func() { opts.MaxConcurrency = int(settings.MaxConcurrency) })
	conditionalSet(settings.MaxIOConcurrency > 0, func() { opts.MaxIOConcurrency = int(settings.MaxIOConcurrency) })

	conditionalSet(settings.TxLogCacheSize > 0, func() { opts.TxLogCacheSize = int(settings.TxLogCacheSize) })
	conditionalSet(settings.VLogMaxOpenedFiles > 0, func() { opts.VLogMaxOpenedFiles = int(settings.VLogMaxOpenedFiles) })
	conditionalSet(settings.TxLogMaxOpenedFiles > 0, func() { opts.TxLogMaxOpenedFiles = int(settings.TxLogMaxOpenedFiles) })
	conditionalSet(settings.CommitLogMaxOpenedFiles > 0, func() { opts.CommitLogMaxOpenedFiles = int(settings.CommitLogMaxOpenedFiles) })

	// index options
	if settings.IndexSettings != nil {
		if opts.IndexOptions == nil {
			opts.IndexOptions = s.defaultIndexOptions()
		}

		conditionalSet(settings.IndexSettings.FlushThreshold > 0, func() {
			opts.IndexOptions.FlushThreshold = int(settings.IndexSettings.FlushThreshold)
		})
		conditionalSet(settings.IndexSettings.SyncThreshold > 0, func() {
			opts.IndexOptions.SyncThreshold = int(settings.IndexSettings.SyncThreshold)
		})
		conditionalSet(settings.IndexSettings.FlushBufferSize > 0, func() {
			opts.IndexOptions.FlushBufferSize = int(settings.IndexSettings.FlushBufferSize)
		})
		conditionalSet(settings.IndexSettings.CleanupPercentage <= 100, func() {
			opts.IndexOptions.CleanupPercentage = int(settings.IndexSettings.CleanupPercentage)
		})
		conditionalSet(settings.IndexSettings.CacheSize > 0, func() {
			opts.IndexOptions.CacheSize = int(settings.IndexSettings.CacheSize)
		})
		conditionalSet(settings.IndexSettings.MaxNodeSize > 0, func() {
			opts.IndexOptions.MaxNodeSize = int(settings.IndexSettings.MaxNodeSize)
		})
		conditionalSet(settings.IndexSettings.MaxActiveSnapshots > 0, func() {
			opts.IndexOptions.MaxActiveSnapshots = int(settings.IndexSettings.MaxActiveSnapshots)
		})
		conditionalSet(settings.IndexSettings.RenewSnapRootAfter > 0, func() {
			opts.IndexOptions.RenewSnapRootAfter = int64(settings.IndexSettings.RenewSnapRootAfter)
		})
		conditionalSet(settings.IndexSettings.CompactionThld > 0, func() {
			opts.IndexOptions.CompactionThld = int(settings.IndexSettings.CompactionThld)
		})
		conditionalSet(settings.IndexSettings.DelayDuringCompaction > 0, func() {
			opts.IndexOptions.DelayDuringCompaction = int64(settings.IndexSettings.DelayDuringCompaction)
		})
		conditionalSet(settings.IndexSettings.NodesLogMaxOpenedFiles > 0, func() {
			opts.IndexOptions.NodesLogMaxOpenedFiles = int(settings.IndexSettings.NodesLogMaxOpenedFiles)
		})
		conditionalSet(settings.IndexSettings.HistoryLogMaxOpenedFiles > 0, func() {
			opts.IndexOptions.HistoryLogMaxOpenedFiles = int(settings.IndexSettings.HistoryLogMaxOpenedFiles)
		})
		conditionalSet(settings.IndexSettings.CommitLogMaxOpenedFiles > 0, func() {
			opts.IndexOptions.CommitLogMaxOpenedFiles = int(settings.IndexSettings.CommitLogMaxOpenedFiles)
		})
	}

	return nil
}

func (opts *dbOptions) isReplicatorRequired() bool {
	return opts.Replica &&
		opts.MasterDatabase != "" &&
		opts.MasterAddress != "" &&
		opts.MasterPort > 0
}

func (s *ImmuServer) saveDBOptions(options *dbOptions) error {
	serializedOptions, err := json.Marshal(options)
	if err != nil {
		return err
	}

	optionsKey := make([]byte, 1+len(options.Database))
	optionsKey[0] = KeyPrefixDBSettings
	copy(optionsKey[1:], []byte(options.Database))

	_, err = s.sysDB.Set(&schema.SetRequest{KVs: []*schema.KeyValue{{Key: optionsKey, Value: serializedOptions}}})

	return err
}

func (s *ImmuServer) loadDBOptions(database string, createIfNotExists bool) (*dbOptions, error) {
	optionsKey := make([]byte, 1+len(database))
	optionsKey[0] = KeyPrefixDBSettings
	copy(optionsKey[1:], []byte(database))

	options := s.defaultDBOptions(database)

	e, err := s.sysDB.Get(&schema.KeyRequest{Key: optionsKey})
	if err == store.ErrKeyNotFound && createIfNotExists {
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
