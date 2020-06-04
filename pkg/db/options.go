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

package db

import (
	"strconv"
	"time"
)

type DbOptions struct {
	dbDir    string
	DbName   string
	sysDbDir string
}

// DefaultOption Initialise Db Optionts to default values
func DefaultOption() *DbOptions {
	ID := strconv.FormatInt(time.Now().UnixNano(), 10)
	return &DbOptions{
		dbDir:    "immudb",
		DbName:   "db_" + ID,
		sysDbDir: "immudbsys",
	}
}

// WithDbName sets dbName
func (o *DbOptions) WithDbName(dbName string) *DbOptions {
	ID := strconv.FormatInt(time.Now().UnixNano(), 10)
	o.DbName = dbName + "_" + ID
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
