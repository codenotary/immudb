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
	"strings"
	"testing"
)

func TestDefaultOptions(t *testing.T) {
	op := DefaultOption()
	if !strings.Contains(op.GetDbName(), "db_") {
		t.Errorf("default db dir non as per convention")
	}
	if op.GetDbDir() != "immudb" {
		t.Errorf("default db name non as per convention")
	}
	if op.GetSysDbDir() != "immudbsys" {
		t.Errorf("default sysdb name non as per convention")
	}
}

func TestSetOptions(t *testing.T) {
	DbName := "Charles_Aznavour"
	op := DefaultOption().WithDbName(DbName)
	if !strings.Contains(op.GetDbName(), DbName) {
		t.Errorf("db name not set correctly , expected %s got %s", DbName, op.GetDbName())
	}
}
