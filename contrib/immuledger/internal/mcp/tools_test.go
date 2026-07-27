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

package mcp

import (
	"slices"
	"testing"
)

// check_compliance searched a keyword set built by ranging over a map and
// breaking at the cap, so Go's randomized map iteration made the answer differ
// run to run — /check could omit the very decision the work violates on one call
// and surface it on the next. The keyword set must be stable.
func TestTaskKeywordsAreDeterministic(t *testing.T) {
	const task = "migrate the billing service off kubernetes onto plain systemd units " +
		"because the operators cannot debug the cluster during an incident"

	want := taskKeywords(task, maxKeywords)
	if len(want) != maxKeywords {
		t.Fatalf("test needs more than %d qualifying words, got %d", maxKeywords, len(want))
	}
	for i := range 20 {
		got := taskKeywords(task, maxKeywords)
		if !slices.Equal(got, want) {
			t.Fatalf("keyword set changed on run %d:\n got %v\nwant %v", i, got, want)
		}
	}
}

// The keywords must be the first qualifying words of the description, in order,
// deduplicated, with short filler words dropped.
func TestTaskKeywordsSelection(t *testing.T) {
	got := taskKeywords("Never use Kubernetes. use kubernetes? no, the cost is too high", 12)
	want := []string{"never", "kubernetes", "kubernetes?", "cost", "high"}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}

	if got := taskKeywords("", 12); got != nil {
		t.Fatalf("expected no keywords for an empty task, got %v", got)
	}
	if got := taskKeywords("alpha beta gamma delta", 2); len(got) != 2 {
		t.Fatalf("cap not honoured: %v", got)
	}
}
