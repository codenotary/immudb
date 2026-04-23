/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestDelta(t *testing.T) {
	tests := []struct {
		name      string
		base      float64
		curr      float64
		want      float64
		tolerance float64
	}{
		{"identical", 100, 100, 0, 1e-9},
		{"5pct_gain", 100, 105, 0.05, 1e-9},
		{"5pct_loss", 100, 95, -0.05, 1e-9},
		{"zero_baseline_any_current", 0, 123.4, 0, 1e-9},
		{"zero_baseline_zero_current", 0, 0, 0, 1e-9},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := delta(tc.base, tc.curr)
			if diff := got - tc.want; diff < -tc.tolerance || diff > tc.tolerance {
				t.Fatalf("delta(%v, %v) = %v, want %v", tc.base, tc.curr, got, tc.want)
			}
		})
	}
}

func TestLoadSuite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "s.json")
	data := []byte(`{
		"benchmarks": [
			{"name": "A", "results": {"txs": 1000, "kvs": 50000}},
			{"name": "B", "results": {"txs": 250, "kvs": 250000}}
		]
	}`)
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}

	s, err := loadSuite(path)
	if err != nil {
		t.Fatalf("loadSuite: %v", err)
	}
	if n := len(s.Benchmarks); n != 2 {
		t.Fatalf("got %d benchmarks, want 2", n)
	}
	if got := s.Benchmarks[0].Name; got != "A" {
		t.Fatalf("benchmark[0].Name = %q, want A", got)
	}
	if got := s.Benchmarks[0].Results.Txs; got != 1000 {
		t.Fatalf("benchmark[0].Results.Txs = %v, want 1000", got)
	}
}

func TestLoadSuiteErrors(t *testing.T) {
	if _, err := loadSuite("/nonexistent/path/to/nowhere.json"); err == nil {
		t.Fatal("expected error for missing file, got nil")
	}

	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.json")
	if err := os.WriteFile(bad, []byte("not valid json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadSuite(bad); err == nil {
		t.Fatal("expected parse error for malformed json, got nil")
	}
}
