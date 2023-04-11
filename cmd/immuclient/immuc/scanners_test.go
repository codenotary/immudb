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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestZScan(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.Set([]string{"key", "val"})
	require.NoError(t, err, "Set fail")

	_, err = ic.Imc.ZAdd([]string{"set", "10.5", "key"})
	require.NoError(t, err, "ZAdd fail")

	msg, err := ic.Imc.ZScan([]string{"set"})
	require.NoError(t, err, "ZScan fail")
	require.Contains(t, msg, "val", "ZScan failed")
}

func TestIScan(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.VerifiedSet([]string{"key", "val"})
	require.NoError(t, err, "Set fail")
}

func TestScan(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.Set([]string{"key", "val"})
	require.NoError(t, err, "Set fail")

	msg, err := ic.Imc.Scan([]string{"k"})
	require.NoError(t, err, "Scan fail")
	require.Contains(t, msg, "val", "Scan failed")
}

func TestCount(t *testing.T) {
	t.SkipNow()

	ic := setupTest(t)

	_, err := ic.Imc.Set([]string{"key", "val"})
	require.NoError(t, err, "Set fail")

	msg, err := ic.Imc.Count([]string{"key"})
	require.NoError(t, err, "Count fail")
	require.Contains(t, msg, "1", "Count failed")
}
