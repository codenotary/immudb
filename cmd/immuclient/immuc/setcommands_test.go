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
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.Set([]string{"key", "val"})

	require.NoError(t, err, "Set fail")
	if !strings.Contains(msg, "value") {
		t.Fatalf("Set failed: %s", msg)
	}
}

func TestVerifiedSet(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.VerifiedSet([]string{"key", "val"})

	require.NoError(t, err, "VerifiedSet fail")
	if !strings.Contains(msg, "value") {
		t.Fatalf("VerifiedSet failed: %s", msg)
	}
}

func TestZAdd(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.VerifiedSet([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := ic.Imc.ZAdd([]string{"val", "1", "key"})

	require.NoError(t, err, "ZAdd fail")
	if !strings.Contains(msg, "hash") {
		t.Fatalf("ZAdd failed: %s", msg)
	}
}

func _TestVerifiedZAdd(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.VerifiedSet([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := ic.Imc.VerifiedZAdd([]string{"val", "1", "key"})

	require.NoError(t, err, "VerifiedZAdd fail")
	if !strings.Contains(msg, "hash") {
		t.Fatalf("VerifiedZAdd failed: %s", msg)
	}
}

func TestCreateDatabase(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.CreateDatabase([]string{"newdb"})
	require.NoError(t, err, "CreateDatabase fail")
	if !strings.Contains(msg, "database successfully created") {
		t.Fatalf("CreateDatabase failed: %s", msg)
	}

	_, err = ic.Imc.DatabaseList([]string{})
	require.NoError(t, err, "DatabaseList fail")

	msg, err = ic.Imc.UseDatabase([]string{"newdb"})
	require.NoError(t, err, "UseDatabase fail")
	if !strings.Contains(msg, "newdb") {
		t.Fatalf("UseDatabase failed: %s", msg)
	}
}
