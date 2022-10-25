/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package immuc_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func cleanupClientFiles() {
	list, _ := filepath.Glob(".state-*")
	for _, e := range list {
		os.Remove(e)
	}

	list, _ = filepath.Glob(".identity-*")
	for _, e := range list {
		os.Remove(e)
	}
}

func TestSet(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.Set([]string{"key", "val"})

	if err != nil {
		t.Fatal("Set fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("Set failed: %s", msg)
	}
}
func TestVerifiedSet(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.VerifiedSet([]string{"key", "val"})

	if err != nil {
		t.Fatal("VerifiedSet fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("VerifiedSet failed: %s", msg)
	}
}
func TestZAdd(t *testing.T) {
	ic := setupTest(t)

	_, _ = ic.Imc.VerifiedSet([]string{"key", "val"})

	msg, err := ic.Imc.ZAdd([]string{"val", "1", "key"})

	if err != nil {
		t.Fatal("ZAdd fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("ZAdd failed: %s", msg)
	}
}
func _TestVerifiedZAdd(t *testing.T) {
	ic := setupTest(t)

	_, _ = ic.Imc.VerifiedSet([]string{"key", "val"})

	msg, err := ic.Imc.VerifiedZAdd([]string{"val", "1", "key"})

	if err != nil {
		t.Fatal("VerifiedZAdd fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("VerifiedZAdd failed: %s", msg)
	}
}
func TestCreateDatabase(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.CreateDatabase([]string{"newdb"})
	if err != nil {
		t.Fatal("CreateDatabase fail", err)
	}
	if !strings.Contains(msg, "database successfully created") {
		t.Fatalf("CreateDatabase failed: %s", msg)
	}

	_, err = ic.Imc.DatabaseList([]string{})
	if err != nil {
		t.Fatal("DatabaseList fail", err)
	}

	msg, err = ic.Imc.UseDatabase([]string{"newdb"})
	if err != nil {
		t.Fatal("UseDatabase fail", err)
	}
	if !strings.Contains(msg, "newdb") {
		t.Fatalf("UseDatabase failed: %s", msg)
	}
}
