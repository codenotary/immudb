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

package database

import (
	"time"

	"github.com/codenotary/immudb/embedded/store"
)

const (
	DefaultDbRootPath          = "./data"
	DefaultReadTxPoolSize      = 128
	DefaultTruncationFrequency = 24 * time.Hour
)

// Options database instance options
type Options struct {
	dbRootPath string

	storeOpts *store.Options

	replica         bool
	syncReplication bool
	syncAcks        int // only if !replica

	readTxPoolSize int
	maxResultSize  int

	// TruncationFrequency determines how frequently to truncate data from the database.
	TruncationFrequency time.Duration

	// RetentionPeriod determines how long to store data in the database.
	RetentionPeriod time.Duration
}

// DefaultOptions Initialise Db Optionts to default values
func DefaultOptions() *Options {
	return &Options{
		dbRootPath:          DefaultDbRootPath,
		storeOpts:           store.DefaultOptions(),
		maxResultSize:       MaxKeyScanLimit,
		readTxPoolSize:      DefaultReadTxPoolSize,
		TruncationFrequency: DefaultTruncationFrequency,
	}
}

// WithDbRootPath sets the directory in which this database will reside
func (o *Options) WithDBRootPath(Path string) *Options {
	o.dbRootPath = Path
	return o
}

// GetDbRootPath returns the directory in which this database resides
func (o *Options) GetDBRootPath() string {
	return o.dbRootPath
}

// WithStoreOptions sets backing store options
func (o *Options) WithStoreOptions(storeOpts *store.Options) *Options {
	o.storeOpts = storeOpts
	return o
}

// GetStoreOptions returns backing store options
func (o *Options) GetStoreOptions() *store.Options {
	return o.storeOpts
}

// AsReplica sets if the database is a replica
func (o *Options) AsReplica(replica bool) *Options {
	o.replica = replica
	return o
}

func (o *Options) WithSyncReplication(syncReplication bool) *Options {
	o.syncReplication = syncReplication
	return o
}

func (o *Options) WithSyncAcks(syncAcks int) *Options {
	o.syncAcks = syncAcks
	return o
}

func (o *Options) WithReadTxPoolSize(txPoolSize int) *Options {
	o.readTxPoolSize = txPoolSize
	return o
}

func (o *Options) GetTxPoolSize() int {
	return o.readTxPoolSize
}

func (o *Options) WithTruncationFrequency(c time.Duration) *Options {
	o.TruncationFrequency = c
	return o
}

func (o *Options) WithRetentionPeriod(c time.Duration) *Options {
	o.RetentionPeriod = c
	return o
}

func (o *Options) WithMaxResultSize(maxResultSize int) *Options {
	o.maxResultSize = maxResultSize
	return o
}
