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

func TestZScan(t *testing.T) {
	cli := setupTest(t)

	_, err := cli.set([]string{"key", "val"})
	require.NoError(t, err, "Set fail")

	_, err = cli.zAdd([]string{"set", "445.3", "key"})
	require.NoError(t, err, "ZAdd fail")

	msg, err := cli.zScan([]string{"set"})
	require.NoError(t, err, "ZScan fail")
	if !strings.Contains(msg, "value") {
		t.Fatalf("ZScan failed: %s", msg)
	}
}

func TestScan(t *testing.T) {
	cli := setupTest(t)

	_, err := cli.set([]string{"key", "val"})
	require.NoError(t, err, "Set fail")

	msg, err := cli.scan([]string{"k"})
	require.NoError(t, err, "Scan fail")
	if !strings.Contains(msg, "value") {
		t.Fatalf("Scan failed: %s", msg)
	}
}

func TestCount(t *testing.T) {
	t.SkipNow()

	cli := setupTest(t)

	_, err := cli.set([]string{"key", "val"})
	require.NoError(t, err, "Set fail")

	msg, err := cli.count([]string{"key"})
	require.NoError(t, err, "Count fail")
	if !strings.Contains(msg, "1") {
		t.Fatalf("Count failed: %s", msg)
	}
}
