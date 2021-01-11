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

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
	"github.com/stretchr/testify/require"
)

type dbMock struct {
	database.DB

	currentStateF func() (*schema.ImmutableState, error)
}

func (dbm dbMock) CurrentState() (*schema.ImmutableState, error) {
	if dbm.currentStateF != nil {
		return dbm.currentStateF()
	}
	return &schema.ImmutableState{TxId: 99}, nil
}

func TestMetricFuncDefaultDBRecordsCounter(t *testing.T) {
	s := ImmuServer{
		dbList: &databaseList{
			databases: []database.DB{dbMock{}},
		},
	}
	nbRecords := s.metricFuncDefaultDBRecordsCounter()
	require.Equal(t, 99, int(nbRecords))
}

func TestMetricFuncServerUptimeCounter(t *testing.T) {
	s := ImmuServer{}
	s.metricFuncServerUptimeCounter()
}

func TestMetricFuncDefaultDBSize(t *testing.T) {
	s := ImmuServer{
		Options: &Options{
			Dir:           ".",
			defaultDbName: "TestMetricFuncServerUptimeCounter_DefaultDB",
		},
	}

	defaultDBPath := filepath.Join(s.Options.Dir, s.Options.defaultDbName)
	require.NoError(t, os.MkdirAll(defaultDBPath, 0777))
	defer os.RemoveAll(defaultDBPath)
	_, err := os.Create(filepath.Join(defaultDBPath, "some-db-file"))
	require.NoError(t, err)

	s.metricFuncDefaultDBSize()
}
