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
	WriteTxHeaderVersion    int `json:"writeTxHeaderVersion"`

	IndexOptions *indexOptions `json:"indexOptions"`

	Autoload featureState `json:"autoload"` // unspecfied is considered as enabled for backward compatibility

	CreatedBy string    `json:"createdBy"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedBy string    `json:"updatedBy"`
	UpdatedAt time.Time `json:"updatedAt"`
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
	FlushThreshold           int     `json:"flushThreshold"`
	SyncThreshold            int     `json:"syncThreshold"`
	FlushBufferSize          int     `json:"flushBufferSize"`
	CleanupPercentage        float32 `json:"cleanupPercentage"`
	CacheSize                int     `json:"cacheSize"`
	MaxNodeSize              int     `json:"maxNodeSize"` // permanent
	MaxActiveSnapshots       int     `json:"maxActiveSnapshots"`
	RenewSnapRootAfter       int64   `json:"renewSnapRootAfter"` // ms
	CompactionThld           int     `json:"compactionThld"`
	DelayDuringCompaction    int64   `json:"delayDuringCompaction"` // ms
	NodesLogMaxOpenedFiles   int     `json:"nodesLogMaxOpenedFiles"`
	HistoryLogMaxOpenedFiles int     `json:"historyLogMaxOpenedFiles"`
	CommitLogMaxOpenedFiles  int     `json:"commitLogMaxOpenedFiles"`
}

const DefaultMaxValueLen = 1 << 25   //32Mb
const DefaultStoreFileSize = 1 << 29 //512Mb

func (s *ImmuServer) defaultDBOptions(database string) *dbOptions {
	dbOpts := &dbOptions{
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
		WriteTxHeaderVersion:    store.DefaultWriteTxHeaderVersion,

		IndexOptions: s.defaultIndexOptions(),

		Autoload: unspecifiedState,

		CreatedAt: time.Now(),
	}

	if dbOpts.Replica && (database == s.Options.systemAdminDBName || database == s.Options.defaultDBName) {
		repOpts := s.Options.ReplicationOptions

		dbOpts.MasterDatabase = dbOpts.Database // replica of systemdb and defaultdb must have the same name as in master
		dbOpts.MasterAddress = repOpts.MasterAddress
		dbOpts.MasterPort = repOpts.MasterPort
		dbOpts.FollowerUsername = repOpts.FollowerUsername
		dbOpts.FollowerPassword = repOpts.FollowerPassword
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
		WithWriteTxHeaderVersion(opts.WriteTxHeaderVersion).
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

func (opts *dbOptions) databaseNullableSettings() *schema.DatabaseNullableSettings {
	return &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:          &schema.NullableBool{Value: opts.Replica},
			MasterDatabase:   &schema.NullableString{Value: opts.MasterDatabase},
			MasterAddress:    &schema.NullableString{Value: opts.MasterAddress},
			MasterPort:       &schema.NullableUint32{Value: uint32(opts.MasterPort)},
			FollowerUsername: &schema.NullableString{Value: opts.FollowerUsername},
			FollowerPassword: &schema.NullableString{Value: opts.FollowerPassword},
		},

		FileSize:     &schema.NullableUint32{Value: uint32(opts.FileSize)},
		MaxKeyLen:    &schema.NullableUint32{Value: uint32(opts.MaxKeyLen)},
		MaxValueLen:  &schema.NullableUint32{Value: uint32(opts.MaxValueLen)},
		MaxTxEntries: &schema.NullableUint32{Value: uint32(opts.MaxTxEntries)},

		ExcludeCommitTime: &schema.NullableBool{Value: opts.ExcludeCommitTime},

		MaxConcurrency:   &schema.NullableUint32{Value: uint32(opts.MaxConcurrency)},
		MaxIOConcurrency: &schema.NullableUint32{Value: uint32(opts.MaxIOConcurrency)},

		TxLogCacheSize:          &schema.NullableUint32{Value: uint32(opts.TxLogCacheSize)},
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
		},

		WriteTxHeaderVersion: &schema.NullableUint32{Value: uint32(opts.WriteTxHeaderVersion)},

		Autoload: &schema.NullableBool{Value: opts.Autoload.isEnabled()},
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

	ret := &schema.DatabaseNullableSettings{
		ReplicationSettings: &schema.ReplicationNullableSettings{
			Replica:          &schema.NullableBool{Value: settings.Replica},
			MasterDatabase:   &schema.NullableString{Value: settings.MasterDatabase},
			MasterAddress:    &schema.NullableString{Value: settings.MasterAddress},
			MasterPort:       &schema.NullableUint32{Value: settings.MasterPort},
			FollowerUsername: &schema.NullableString{Value: settings.FollowerUsername},
			FollowerPassword: &schema.NullableString{Value: settings.FollowerPassword},
		},
		FileSize:     nullableUInt32(settings.FileSize),
		MaxKeyLen:    nullableUInt32(settings.MaxKeyLen),
		MaxValueLen:  nullableUInt32(settings.MaxValueLen),
		MaxTxEntries: nullableUInt32(settings.MaxTxEntries),
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

		if settings.IndexSettings != nil && settings.IndexSettings.MaxNodeSize != nil {
			return fmt.Errorf("%w: %s can not be changed after database creation ('%s')", ErrIllegalArguments, "max node size", opts.Database)
		}

		opts.UpdatedAt = time.Now()
	}

	opts.synced = s.Options.synced

	if settings.ReplicationSettings != nil {
		rs := settings.ReplicationSettings

		if rs.Replica != nil {
			opts.Replica = rs.Replica.Value
		}
		if rs.MasterDatabase != nil {
			opts.MasterDatabase = rs.MasterDatabase.Value
		}
		if rs.MasterAddress != nil {
			opts.MasterAddress = rs.MasterAddress.Value
		}
		if rs.MasterPort != nil {
			opts.MasterPort = int(rs.MasterPort.Value)
		}
		if rs.FollowerUsername != nil {
			opts.FollowerUsername = rs.FollowerUsername.Value
		}
		if rs.FollowerPassword != nil {
			opts.FollowerPassword = rs.FollowerPassword.Value
		}
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

	// store options
	if settings.ExcludeCommitTime != nil {
		opts.ExcludeCommitTime = settings.ExcludeCommitTime.Value
	}

	if settings.MaxConcurrency != nil {
		opts.MaxConcurrency = int(settings.MaxConcurrency.Value)
	}
	if settings.MaxIOConcurrency != nil {
		opts.MaxIOConcurrency = int(settings.MaxIOConcurrency.Value)
	}

	if settings.TxLogCacheSize != nil {
		opts.TxLogCacheSize = int(settings.TxLogCacheSize.Value)
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
	}

	err := opts.Validate()
	if err != nil {
		return fmt.Errorf("%w: %v", ErrIllegalArguments, err)
	}

	return nil
}

func (opts *dbOptions) Validate() error {
	if !opts.Replica &&
		(opts.MasterDatabase != "" ||
			opts.MasterAddress != "" ||
			opts.MasterPort > 0 ||
			opts.FollowerUsername != "" ||
			opts.FollowerPassword != "") {
		return fmt.Errorf("%w: invalid replication options for database '%s'", ErrIllegalArguments, opts.Database)
	}

	return opts.storeOptions().Validate()
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

func (s *ImmuServer) deleteDBOptionsFor(db string) error {
	optionsKey := make([]byte, 1+len(db))
	optionsKey[0] = KeyPrefixDBSettings
	copy(optionsKey[1:], []byte(db))

	_, err := s.sysDB.Delete(&schema.DeleteKeysRequest{
		Keys: [][]byte{
			optionsKey,
		},
	})

	return err
}

func (s *ImmuServer) loadDBOptions(database string, createIfNotExists bool) (*dbOptions, error) {
	if database == s.Options.systemAdminDBName || database == s.Options.defaultDBName {
		return s.defaultDBOptions(database), nil
	}

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

func (s *ImmuServer) logDBOptions(database string, opts *dbOptions) {
	// This list is manually updated to ensure we don't expose sensitive information
	// in logs such as replication passwords
	s.Logger.Infof("%s.Autoload: %v", database, opts.Autoload.isEnabled())
	s.Logger.Infof("%s.Synced: %v", database, opts.synced)
	s.Logger.Infof("%s.Replica: %v", database, opts.Replica)
	s.Logger.Infof("%s.FileSize: %v", database, opts.FileSize)
	s.Logger.Infof("%s.MaxKeyLen: %v", database, opts.MaxKeyLen)
	s.Logger.Infof("%s.MaxValueLen: %v", database, opts.MaxValueLen)
	s.Logger.Infof("%s.MaxTxEntries: %v", database, opts.MaxTxEntries)
	s.Logger.Infof("%s.ExcludeCommitTime: %v", database, opts.ExcludeCommitTime)
	s.Logger.Infof("%s.MaxConcurrency: %v", database, opts.MaxConcurrency)
	s.Logger.Infof("%s.MaxIOConcurrency: %v", database, opts.MaxIOConcurrency)
	s.Logger.Infof("%s.TxLogCacheSize: %v", database, opts.TxLogCacheSize)
	s.Logger.Infof("%s.VLogMaxOpenedFiles: %v", database, opts.VLogMaxOpenedFiles)
	s.Logger.Infof("%s.TxLogMaxOpenedFiles: %v", database, opts.TxLogMaxOpenedFiles)
	s.Logger.Infof("%s.CommitLogMaxOpenedFiles: %v", database, opts.CommitLogMaxOpenedFiles)
	s.Logger.Infof("%s.WriteTxHeaderVersion: %v", database, opts.WriteTxHeaderVersion)
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
}
