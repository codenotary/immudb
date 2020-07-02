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
	"testing"
)

func TestDefaultOptions(t *testing.T) {
	op := DefaultOption()
	if op.GetDbName() != "db_name" {
		t.Errorf("default sysdb name not what expected")
	}
	if op.GetDbRootPath() != DefaultOptions().Dir {
		t.Errorf("default db rootpath not what expected")
	}
	if !op.GetCorruptionChecker() {
		t.Errorf("default corruption checker not what expected")
	}
	if op.GetInMemoryStore() {
		t.Errorf("default in memory store not what expected")
	}

	DbName := "Charles_Aznavour"
	rootpath := "rootpath"
	op = DefaultOption().WithDbName(DbName).
		WithDbRootPath(rootpath).WithCorruptionChecker(false).WithInMemoryStore(true)
	if op.GetDbName() != DbName {
		t.Errorf("db name not set correctly , expected %s got %s", DbName, op.GetDbName())
	}
	if op.GetDbRootPath() != rootpath {
		t.Errorf("rootpath not set correctly , expected %s got %s", rootpath, op.GetDbRootPath())
	}

	if op.GetCorruptionChecker() {
		t.Errorf("coruuption checker not set correctly , expected %v got %v", false, op.GetCorruptionChecker())
	}
	if !op.GetInMemoryStore() {
		t.Errorf("in  memory store not set correctly , expected %v got %v", false, op.GetInMemoryStore())
	}
}
