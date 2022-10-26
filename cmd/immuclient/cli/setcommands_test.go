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

package cli

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestSet(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.set([]string{"key", "val"})
	require.NoError(t, err, "Set fail")
	if !strings.Contains(msg, "value") {
		t.Fatalf("Set failed: %s", msg)
	}
}

func TestSafeSet(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.safeset([]string{"key", "val"})
	require.NoError(t, err, "SafeSet fail")
	if !strings.Contains(msg, "value") {
		t.Fatalf("SafeSet failed: %s", msg)
	}
}

func TestZAdd(t *testing.T) {
	cli := setupTest(t)

	_, err := cli.safeset([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := cli.zAdd([]string{"val", "1", "key"})

	require.NoError(t, err, "ZAdd fail")
	if !strings.Contains(msg, "hash") {
		t.Fatalf("ZAdd failed: %s", msg)
	}
}

func TestSafeZAdd(t *testing.T) {
	cli := setupTest(t)

	_, err := cli.safeset([]string{"key", "val"})
	require.NoError(t, err)

	msg, err := cli.safeZAdd([]string{"val", "1", "key"})

	require.NoError(t, err, "SafeZAdd fail")
	if !strings.Contains(msg, "hash") {
		t.Fatalf("SafeZAdd failed: %s", msg)
	}
}
