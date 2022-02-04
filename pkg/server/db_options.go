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
	Synced                bool  `json:"synced"`
	FlushThreshold        int   `json:"flushThreshold"`
	SyncThreshold         int   `json:"syncThreshold"`
	CacheSize             int   `json:"cacheSize"`
	MaxNodeSize           int   `json:"maxNodeSize"` // permanent
	MaxActiveSnapshots    int   `json:"maxActiveSnapshots"`
	RenewSnapRootAfter    int64 `json:"renewSnapRootAfter"`
	CompactionThld        int   `json:"compactionThld"`
	DelayDuringCompaction int64 `json:"delayDuringCompaction"`
}

const DefaultMaxValueLen = 1 << 25   //32Mb
const DefaultStoreFileSize = 1 << 29 //512Mb

func (s *ImmuServer) defaultDBOptions(database string) *dbOptions {
	return &dbOptions{
		Database: database,
		synced:   s.Options.synced,
		Replica:  s.Options.ReplicationOptions != nil,

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
		Synced:                false,
		FlushThreshold:        tbtree.DefaultFlushThld,
		SyncThreshold:         tbtree.DefaultSyncThld,
		CacheSize:             tbtree.DefaultCacheSize,
		MaxNodeSize:           tbtree.DefaultMaxNodeSize,
		MaxActiveSnapshots:    tbtree.DefaultMaxActiveSnapshots,
		RenewSnapRootAfter:    tbtree.DefaultRenewSnapRootAfter.Milliseconds(),
		CompactionThld:        tbtree.DefaultCompactionThld,
		DelayDuringCompaction: tbtree.DefaultDelayDuringCompaction.Milliseconds(),
	}
}

func (s *ImmuServer) databaseOptionsFrom(opts *dbOptions) *database.Options {
	return database.DefaultOption().
		WithDBName(opts.Database).
		WithDBRootPath(s.Options.Dir).
		WithStoreOptions(s.storeOptionsForDB(opts.Database, s.remoteStorage, opts.storeOptions())).
		AsReplica(opts.Replica)
}

func (opts *dbOptions) storeOptions() *store.Options {
	indexOpts := store.DefaultIndexOptions()

	if opts.IndexOptions != nil {
		indexOpts.WithSynced(opts.IndexOptions.Synced).
			WithFlushThld(opts.IndexOptions.FlushThreshold).
			WithSyncThld(opts.IndexOptions.SyncThreshold).
			WithCacheSize(opts.IndexOptions.CacheSize).
			WithMaxNodeSize(opts.IndexOptions.MaxNodeSize).
			WithMaxActiveSnapshots(opts.IndexOptions.MaxActiveSnapshots).
			WithRenewSnapRootAfter(time.Millisecond * time.Duration(opts.IndexOptions.RenewSnapRootAfter)).
			WithCompactionThld(opts.IndexOptions.CompactionThld).
			WithDelayDuringCompaction(time.Millisecond * time.Duration(opts.IndexOptions.DelayDuringCompaction))
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
		WithIndexOptions(indexOpts)

	if opts.ExcludeCommitTime {
		stOpts.WithTimeFunc(func() time.Time { return time.Unix(0, 0) })
	} else {
		stOpts.WithTimeFunc(func() time.Time { return time.Now() })
	}

	return stOpts
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
		if settings.FileSize > 0 {
			return fmt.Errorf("%w: file size can not be modified", ErrIllegalArguments)
		}

		if settings.MaxKeyLen > 0 {
			return fmt.Errorf("%w: max key lenght can not be modified", ErrIllegalArguments)
		}

		if settings.MaxValueLen > 0 {
			return fmt.Errorf("%w: max value lenght can not be modified", ErrIllegalArguments)
		}

		if settings.MaxTxEntries > 0 {
			return fmt.Errorf("%w: max number of entries per transaction can not be modified", ErrIllegalArguments)
		}

		if settings.IndexSettings != nil && settings.IndexSettings.MaxNodeSize > 0 {
			return fmt.Errorf("%w: max node size can not be modified", ErrIllegalArguments)
		}
	}

	opts.Replica = settings.Replica
	opts.MasterDatabase = settings.MasterDatabase
	opts.MasterAddress = settings.MasterAddress
	opts.MasterPort = int(settings.MasterPort)
	opts.FollowerUsername = settings.FollowerUsername
	opts.FollowerPassword = settings.FollowerPassword

	// store options
	opts.ExcludeCommitTime = settings.ExcludeCommitTime

	conditionalSet(settings.MaxConcurrency > 0, func() { opts.MaxConcurrency = int(settings.MaxConcurrency) })
	conditionalSet(settings.MaxIOConcurrency > 0, func() { opts.MaxIOConcurrency = int(settings.MaxIOConcurrency) })
	conditionalSet(settings.TxLogCacheSize > 0, func() { opts.TxLogCacheSize = int(settings.TxLogCacheSize) })
	conditionalSet(settings.VLogMaxOpenedFiles > 0, func() { opts.VLogMaxOpenedFiles = int(settings.VLogMaxOpenedFiles) })
	conditionalSet(settings.TxLogMaxOpenedFiles > 0, func() { opts.TxLogMaxOpenedFiles = int(settings.TxLogMaxOpenedFiles) })
	conditionalSet(settings.CommitLogMaxOpenedFiles > 0, func() { opts.MaxConcurrency = int(settings.CommitLogMaxOpenedFiles) })

	// index options
	if settings.IndexSettings != nil {
		if opts.IndexOptions == nil {
			opts.IndexOptions = s.defaultIndexOptions()
		}

		opts.IndexOptions.Synced = settings.IndexSettings.Synced

		conditionalSet(settings.IndexSettings.FlushThreshold > 0, func() {
			opts.IndexOptions.FlushThreshold = int(settings.IndexSettings.FlushThreshold)
		})
		conditionalSet(settings.IndexSettings.SyncThreshold > 0, func() {
			opts.IndexOptions.SyncThreshold = int(settings.IndexSettings.SyncThreshold)
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
	}

	opts.UpdatedAt = time.Now()

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

	e, err := s.sysDB.Get(&schema.KeyRequest{Key: optionsKey})
	if err == store.ErrKeyNotFound && createIfNotExists {
		options := s.defaultDBOptions(database)
		err = s.saveDBOptions(options)
		if err != nil {
			return nil, err
		}
		return options, nil
	}
	if err != nil {
		return nil, err
	}

	var options *dbOptions
	err = json.Unmarshal(e.Value, &options)
	if err != nil {
		return nil, err
	}

	return options, nil
}
