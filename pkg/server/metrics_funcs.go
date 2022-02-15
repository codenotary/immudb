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
	"time"
)

func (s *ImmuServer) metricFuncServerUptimeCounter() float64 {
	return time.Since(startedAt).Hours()
}

// returns the specified directory's size in bytes
func dirSize(dir string) (int64, error) {
	var dirSizeBytes int64 = 0
	addSizeIfNotDir := func(path string, file os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if file.IsDir() {
			return nil
		}
		dirSizeBytes += file.Size()
		return nil
	}
	if err := filepath.Walk(dir, addSizeIfNotDir); err != nil {
		return 0, fmt.Errorf(
			"error walking dir %s to read it's size: %v", dir, err)
	}
	return dirSizeBytes, nil
}

func (s *ImmuServer) metricFuncComputeDBSizes() (dbSizes map[string]float64) {
	dbSizes = make(map[string]float64)

	if s.dbList != nil {
		for i := 0; i < s.dbList.Length(); i++ {
			db := s.dbList.GetByIndex(int64(i))
			dbName := db.GetName()
			dbSize, err := dirSize(filepath.Join(s.Options.Dir, dbName))
			if err != nil {
				s.Logger.Errorf("error updating db size metric for db %s: %v", dbName, err)
				continue
			}
			dbSizes[dbName] = float64(dbSize)
		}
	} else {
		s.Logger.Warningf(
			"current update of db sizes metrics for regular dbs was skipped: db list is nil")
	}

	// add systemdb
	if s.sysDB != nil {
		sysDBName := s.sysDB.GetName()
		sysDBSize, err := dirSize(filepath.Join(s.Options.Dir, sysDBName))
		if err != nil {
			s.Logger.Errorf("error updating db size metric for system db %s: %v", sysDBName, err)
		} else {
			dbSizes[sysDBName] = float64(sysDBSize)
		}
	} else {
		s.Logger.Warningf(
			"current update of db size metric for system db was skipped: system db is nil")
	}

	return
}

func (s *ImmuServer) metricFuncComputeDBEntries() (nbEntriesPerDB map[string]float64) {
	nbEntriesPerDB = make(map[string]float64)

	if s.dbList != nil {
		for i := 0; i < s.dbList.Length(); i++ {
			db := s.dbList.GetByIndex(int64(i))
			dbName := db.GetName()
			state, err := db.CurrentState()
			if err != nil {
				s.Logger.Errorf(
					"error getting current state of db %s to update the number of entries metric: %v",
					dbName, err)
				continue
			}
			nbEntriesPerDB[dbName] = float64(state.GetTxId())
		}
	} else {
		s.Logger.Warningf(
			"current update of db entries metrics for regular dbs was skipped: db list is nil")
	}

	// add systemdb
	if s.sysDB != nil {
		sysDBName := s.sysDB.GetName()
		state, err := s.sysDB.CurrentState()
		if err != nil {
			s.Logger.Errorf(
				"error getting current state of system db %s to update the number of entries metric: %v",
				sysDBName, err)
		} else {
			nbEntriesPerDB[sysDBName] = float64(state.GetTxId())
		}
	} else {
		s.Logger.Warningf(
			"current update of db entries metric for system db was skipped: system db is nil")
	}

	return
}
