/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package database

import "github.com/codenotary/immudb/embedded/store"

//Options database instance options
type Options struct {
	dbName     string
	dbRootPath string
	storeOpts  *store.Options

	corruptionChecker bool

	replica         bool
	replicationOpts *ReplicationOptions
}

type ReplicationOptions struct {
	SrcDatabase string
	SrcAddress  string
	SrcPort     int
	FollowerUsr string
	FollowerPwd string
}

// DefaultOption Initialise Db Optionts to default values
func DefaultOption() *Options {
	return &Options{
		dbRootPath:      "./data",
		dbName:          "db_name",
		storeOpts:       store.DefaultOptions(),
		replicationOpts: &ReplicationOptions{},
	}
}

// WithDbName sets dbName, which is also db instance directory
func (o *Options) WithDBName(dbName string) *Options {
	o.dbName = dbName
	return o
}

// GetDbName Returns Database name which is also db instance directory
func (o *Options) GetDBName() string {
	return o.dbName
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

// WithCorruptionChecker sets if corruption checker should start for this database instance
func (o *Options) WithCorruptionChecker(cc bool) *Options {
	o.corruptionChecker = cc
	return o
}

// GetCorruptionChecker returns if corruption checker should start for this database instance
func (o *Options) GetCorruptionChecker() bool {
	return o.corruptionChecker
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

// GetReplicationOptions returns replication options
func (o *Options) GetReplicationOptions() *ReplicationOptions {
	return o.replicationOpts
}

// WithReplicationOptions sets replication options
func (o *Options) WithReplicationOptions(replicationOpts *ReplicationOptions) *Options {
	o.replicationOpts = replicationOpts
	return o
}

// AsReplica sets if the database is a replica
func (o *Options) AsReplica(replica bool) *Options {
	o.replica = replica
	return o
}

// ReplicationOptions -------

// WithSrcDatabase sets the source database name
func (o *ReplicationOptions) WithSrcDatabase(srcDatabase string) *ReplicationOptions {
	o.SrcDatabase = srcDatabase
	return o
}

// WithSrcAddress sets the source database address
func (o *ReplicationOptions) WithSrcAddress(srcAddress string) *ReplicationOptions {
	o.SrcAddress = srcAddress
	return o
}

// WithSrcPort sets the source database port
func (o *ReplicationOptions) WithSrcPort(srcPort int) *ReplicationOptions {
	o.SrcPort = srcPort
	return o
}

// WithFollowerUsr sets follower username
func (o *ReplicationOptions) WithFollowerUsr(followerUsr string) *ReplicationOptions {
	o.FollowerUsr = followerUsr
	return o
}

// WithFollowerPwd sets follower password
func (o *ReplicationOptions) WithFollowerPwd(followerPwd string) *ReplicationOptions {
	o.FollowerPwd = followerPwd
	return o
}
