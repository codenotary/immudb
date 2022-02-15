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

package server

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/cmdtest"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/codenotary/immudb/pkg/logger"
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
	return database.DefaultOption()
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
	dbList := database.NewDatabaseList()
	dbList.Append(dbMock{
		currentStateF: func() (*schema.ImmutableState, error) {
			return currentStateSuccessfulOnce(&currentStateCounter)
		},
	})

	currentStateCountersysDB := 0
	sysDB := dbMock{
		getNameF: func() string {
			return "systemdb"
		},
		getOptionsF: func() *database.Options {
			return database.DefaultOption()
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
	dataDir := "TestDBSizesData"
	defaultDBName := "TestDBSizesDefaultDB"

	//--> create the data dir with subdir for each db
	var fullPermissions os.FileMode = 0777
	require.NoError(t, os.MkdirAll(dataDir, fullPermissions))
	defer os.RemoveAll(dataDir)

	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, defaultDBName), fullPermissions))
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, SystemDBName), fullPermissions))
	require.NoError(t, os.MkdirAll(filepath.Join(dataDir, SystemDBName, "some-dir"), fullPermissions))
	file, err := os.Create(filepath.Join(dataDir, defaultDBName, "some-file"))
	require.NoError(t, err)
	defer file.Close()
	//<--

	dbList := database.NewDatabaseList()
	dbList.Append(dbMock{
		getNameF: func() string {
			return "defaultdb"
		},
		getOptionsF: func() *database.Options {
			return database.DefaultOption()
		},
	})

	s := ImmuServer{
		Options: &Options{
			Dir:           dataDir,
			defaultDBName: defaultDBName,
		},
		dbList: dbList,
		sysDB: dbMock{
			getOptionsF: func() *database.Options {
				return database.DefaultOption()
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
