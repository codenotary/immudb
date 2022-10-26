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

func TestHealthCheck(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.healthCheck([]string{})
	require.NoError(t, err, "HealthCheck fail")
	if !strings.Contains(msg, "Health check OK") {
		t.Fatal("HealthCheck fail")
	}
}

func TestHistory(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.history([]string{"key"})
	require.NoError(t, err, "History fail")
	if !strings.Contains(msg, "key not found") {
		t.Fatalf("History fail %s", msg)
	}

	_, err = cli.set([]string{"key", "value"})
	require.NoError(t, err, "History fail")

	msg, err = cli.history([]string{"key"})
	require.NoError(t, err, "History fail")
	if !strings.Contains(msg, "value") {
		t.Fatalf("History fail %s", msg)
	}
}

func TestVersion(t *testing.T) {
	cli := setupTest(t)

	msg, err := cli.version([]string{"key"})
	require.NoError(t, err, "version fail")
	if !strings.Contains(msg, "no version info available") {
		t.Fatalf("version fail %s", msg)
	}
}
