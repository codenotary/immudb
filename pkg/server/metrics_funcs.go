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
	"os"
	"path/filepath"
	"time"
)

func (s *ImmuServer) metricFuncDefaultDBRecordsCounter() float64 {
	ic, err := s.dbList.GetByIndex(DefaultDbIndex).CurrentState()
	if err != nil {
		return 0
	}
	return float64(ic.GetTxId())
}

func (s *ImmuServer) metricFuncServerUptimeCounter() float64 {
	return time.Since(startedAt).Hours()
}

func (s *ImmuServer) metricFuncDefaultDBSize() float64 {
	var defaultDBDirSizeBytes int64 = 0
	readSize := func(path string, file os.FileInfo, err error) error {
		if !file.IsDir() {
			defaultDBDirSizeBytes += file.Size()
		}
		return nil
	}
	defaultDBPath := filepath.Join(s.Options.Dir, s.Options.defaultDbName)
	filepath.Walk(defaultDBPath, readSize)
	return float64(defaultDBDirSizeBytes)
}
