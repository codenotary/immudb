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
	"os"
	"path"
	"testing"
)

func TestDefaultDbCreation(t *testing.T) {
	options := DefaultOption()
	_, err := NewDb(options)
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer os.RemoveAll(options.DbName)

	if _, err = os.Stat(options.DbName); os.IsNotExist(err) {
		t.Errorf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Data dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetSysDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Sys dir not created")
	}
}
func TestDbCreation(t *testing.T) {
	options := DefaultOption().WithDbName("EdithPiaf")
	_, err := NewDb(options)
	if err != nil {
		t.Errorf("Error creating Db instance %s", err)
	}
	defer os.RemoveAll(options.DbName)

	if _, err = os.Stat(options.DbName); os.IsNotExist(err) {
		t.Errorf("Db dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Data dir not created")
	}

	_, err = os.Stat(path.Join(options.DbName, options.GetSysDbDir()))
	if os.IsNotExist(err) {
		t.Errorf("Sys dir not created")
	}
}
