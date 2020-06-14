/*
Copyright 2019-2020 vChain, Inc.

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

type DbOptions struct {
	dbDir      string
	dbName     string
	sysDbDir   string
	dbRootPath string
}

// DefaultOption Initialise Db Optionts to default values
func DefaultOption() *DbOptions {
	return &DbOptions{
		dbDir:      "immudb",
		dbName:     "db_" + GenerateDbID(),
		sysDbDir:   "immudbsys",
		dbRootPath: "",
	}
}

// WithDbName sets dbName, which is also db instance directory
func (o *DbOptions) WithDbName(dbName string) *DbOptions {
	o.dbName = dbName
	return o
}

// GetDbDir Returns Database Directory name
func (o *DbOptions) GetDbDir() string {
	return o.dbDir
}

// GetSysDbDir Returns System Database Directory name
func (o *DbOptions) GetSysDbDir() string {
	return o.sysDbDir
}

// GetDbName Returns Database name which is also db instance directory
func (o *DbOptions) GetDbName() string {
	return o.dbName
}

// DbRootPath sets the directory in which this database will reside
func (o *DbOptions) WithDbRootPath(Path string) *DbOptions {
	o.dbRootPath = Path
	return o
}

// GetDbRootPath returns the directory in which this database resides
func (o *DbOptions) GetDbRootPath() string {
	return o.dbRootPath
}
