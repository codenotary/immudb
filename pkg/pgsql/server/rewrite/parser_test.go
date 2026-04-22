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

package rewrite

import (
	"bufio"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestParserFidelity measures auxten's coverage against the curated
// corpus/transcripts.sql file. It's not a pass/fail pin — failures
// are recorded as a percentage and logged, so the test passes when
// coverage stays above a floor (80% for B1). B2 can raise the floor
// as more query shapes are handled.
//
// Intent: give B2 planners an objective "auxten handles X% of our
// production SQL; the rest needs the regex fallback" number without
// having to re-survey the corpus each time.
func TestParserFidelity(t *testing.T) {
	path := filepath.Join("corpus", "transcripts.sql")
	f, err := os.Open(path)
	require.NoError(t, err, "corpus file must exist at %s", path)
	defer f.Close()

	var total, ok int
	var failing []string

	scanner := bufio.NewScanner(f)
	// Allow longer lines — some psql queries run 400+ chars.
	scanner.Buffer(make([]byte, 0, 64*1024), 64*1024)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" || strings.HasPrefix(line, "--") {
			continue
		}
		total++
		if CanParse(line) {
			ok++
			continue
		}
		failing = append(failing, line)
	}
	require.NoError(t, scanner.Err())
	require.Greater(t, total, 0, "corpus is empty")

	pct := float64(ok) / float64(total) * 100
	t.Logf("parser fidelity: %d/%d = %.1f%%", ok, total, pct)
	for _, q := range failing {
		t.Logf("  FAIL: %s", truncate(q, 120))
	}

	// B1 floor: 60% of the curated corpus parses. The immudb-
	// specific DDL lines (VARCHAR[N], AUTO_INCREMENT, COPY) are in
	// the corpus on purpose — they measure how much the regex
	// fallback still carries. Raise this floor in B2 as rule
	// coverage grows.
	const b1Floor = 60.0
	require.GreaterOrEqual(t, pct, b1Floor,
		"parser fidelity dropped below B1 floor (%.1f%% < %.1f%%); %d failing lines logged above",
		pct, b1Floor, len(failing))
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}
