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
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/cmdtest"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
)

type dbMock struct {
	database.DB

	currentStateF func() (*schema.ImmutableState, error)
	getOptionsF   func() *database.Options
	getNameF      func() string
}

func (dbm dbMock) CurrentState() (*schema.ImmutableState, error) {
	if dbm.currentStateF != nil {
		return dbm.currentStateF()
	}
	return &schema.ImmutableState{TxId: 99}, nil
}

func (dbm dbMock) GetOptions() *database.Options {
	if dbm.getOptionsF != nil {
		return dbm.getOptionsF()
	}
	return database.DefaultOptions()
}

func (dbm dbMock) GetName() string {
	if dbm.getNameF != nil {
		return dbm.getNameF()
	}
	return ""
}

func TestMetricFuncComputeDBEntries(t *testing.T) {
	currentStateSuccessfulOnce := func(callCounter *int) (*schema.ImmutableState, error) {
		*callCounter++
		if *callCounter == 1 {
			return &schema.ImmutableState{TxId: 99}, nil
		} else {
			return nil, fmt.Errorf(
				"some current state error %d", *callCounter)
		}
	}

	currentStateCounter := 0
	dbList := database.NewDatabaseList(database.NewDBManager(func(name string, opts *database.Options) (database.DB, error) {
		return &dbMock{
			currentStateF: func() (*schema.ImmutableState, error) {
				return currentStateSuccessfulOnce(&currentStateCounter)
			},
		}, nil
	}, 100, logger.NewMemoryLogger()))

	dbList.Put("test", database.DefaultOptions())

	currentStateCountersysDB := 0
	sysDB := dbMock{
		getNameF: func() string {
			return "systemdb"
		},
		getOptionsF: func() *database.Options {
			return database.DefaultOptions()
		},
		currentStateF: func() (*schema.ImmutableState, error) {
			return currentStateSuccessfulOnce(&currentStateCountersysDB)
		},
	}

	var sw strings.Builder
	s := ImmuServer{
		dbList: dbList,
		sysDB:  sysDB,
		Logger: logger.NewSimpleLoggerWithLevel(
			"TestMetricFuncComputeDBSizes",
			&sw,
			logger.LogError),
	}

	nbEntriesPerDB := s.metricFuncComputeDBEntries()
	require.Len(t, nbEntriesPerDB, 2)

	// call once again catch the currentState error paths
	s.metricFuncComputeDBEntries()

	// test warning paths (when dbList and sysDB are nil)
	s.dbList = nil
	s.sysDB = nil
	s.metricFuncComputeDBEntries()
}

func TestMetricFuncServerUptimeCounter(t *testing.T) {
	s := ImmuServer{}
	s.metricFuncServerUptimeCounter()
}

func TestMetricFuncComputeDBSizes(t *testing.T) {
	dataDir := filepath.Join(t.TempDir(), "TestDBSizesData")
	defaultDBName := "TestDBSizesDefaultDB"

	//--> create the data dir with subdir for each db
	var fullPermissions os.FileMode = 0777
	require.NoError(t, os.MkdirAll(dataDir, fullPermissions))
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, defaultDBName), fullPermissions))
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, SystemDBName), fullPermissions))
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, SystemDBName, "some-dir"), fullPermissions))
	file, err := os.Create(filepath.Join(dataDir, defaultDBName, "some-file"))
	require.NoError(t, err)
	defer file.Close()
	//<--

	dbList := database.NewDatabaseList(database.NewDBManager(func(name string, opts *database.Options) (database.DB, error) {
		return &dbMock{
			getNameF: func() string {
				return "defaultdb"
			},
			getOptionsF: func() *database.Options {
				return database.DefaultOptions()
			},
		}, nil
	}, 100, logger.NewMemoryLogger()))
	dbList.Put("test", database.DefaultOptions())

	s := ImmuServer{
		Options: &Options{
			Dir:           dataDir,
			defaultDBName: defaultDBName,
		},
		dbList: dbList,
		sysDB: dbMock{
			getOptionsF: func() *database.Options {
				return database.DefaultOptions()
			},
		},
	}

	var sw strings.Builder
	s.Logger = logger.NewSimpleLoggerWithLevel(
		"TestMetricFuncComputeDBSizes",
		&sw,
		logger.LogError)

	s.metricFuncComputeDBSizes()

	// non-existent dir
	s.Options.Dir = cmdtest.RandString()
	s.metricFuncComputeDBSizes()

	// test warning paths (when dbList and sysDB are nil)
	s.dbList = nil
	s.sysDB = nil
	s.metricFuncComputeDBSizes()
}

func TestMetricFuncComputeLoadedDBSize(t *testing.T) {
	dbList := database.NewDatabaseList(database.NewDBManager(func(name string, opts *database.Options) (database.DB, error) {
		db := dbMock{
			getNameF: func() string {
				return name
			},
			getOptionsF: func() *database.Options {
				return opts
			},
		}
		return db, nil
	}, 10, logger.NewMemoryLogger()))

	dbList.Put("defaultdb", database.DefaultOptions())

	var sw strings.Builder
	s := ImmuServer{
		Options: &Options{
			defaultDBName: "defaultdb",
		},
		dbList: dbList,
		sysDB: dbMock{
			getOptionsF: func() *database.Options {
				return database.DefaultOptions()
			},
		},
		Logger: logger.NewSimpleLoggerWithLevel(
			"TestMetricFuncComputeDBSizes",
			&sw,
			logger.LogError),
	}
	require.Equal(t, s.metricFuncComputeLoadedDBSize(), 1.0)
	require.Equal(t, s.metricFuncComputeSessionCount(), 0.0)
}
