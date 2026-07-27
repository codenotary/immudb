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

package ledger

import (
	"context"
	"strings"
	"testing"

	"github.com/codenotary/immudb/pkg/database"
)

func TestLedgerLifecycle(t *testing.T) {
	ctx := context.Background()
	l := New(t.TempDir())
	const project = "demo"

	// Record a decision.
	d, err := l.RecordDecision(ctx, project, Decision{
		Title:     "Use PASETO over JWT",
		Rationale: "Simpler, safer defaults; no alg-confusion footgun.",
		Tags:      "auth,security",
	})
	if err != nil {
		t.Fatalf("RecordDecision: %v", err)
	}
	if d.ID != 1 || d.Status != statusActive {
		t.Fatalf("unexpected decision: %+v", d)
	}

	// Get it back.
	got, err := l.GetDecision(ctx, project, d.ID)
	if err != nil || got == nil {
		t.Fatalf("GetDecision: %v (nil=%v)", err, got == nil)
	}
	if got.Title != d.Title {
		t.Fatalf("title mismatch: %q", got.Title)
	}

	// Search finds it.
	found, err := l.SearchDecisions(ctx, project, "paseto", 10)
	if err != nil || len(found) != 1 {
		t.Fatalf("SearchDecisions: %v count=%d", err, len(found))
	}

	// Supersede it.
	d2, err := l.RecordDecision(ctx, project, Decision{
		Title:      "Keep JWT for third-party interop",
		Rationale:  "A partner requires RS256 JWT; scope limited to that edge.",
		Tags:       "auth",
		Supersedes: d.ID,
	})
	if err != nil {
		t.Fatalf("RecordDecision(supersede): %v", err)
	}
	if d2.ID != 2 {
		t.Fatalf("expected id 2, got %d", d2.ID)
	}

	old, err := l.GetDecision(ctx, project, d.ID)
	if err != nil || old == nil {
		t.Fatalf("GetDecision(old): %v", err)
	}
	if old.Status != statusRetired || old.SupersededBy != d2.ID {
		t.Fatalf("old decision not retired: %+v", old)
	}

	// Active list shows only the new one.
	active, err := l.ListDecisions(ctx, project, "active", "", 50)
	if err != nil {
		t.Fatalf("ListDecisions: %v", err)
	}
	if len(active) != 1 || active[0].ID != d2.ID {
		t.Fatalf("active list wrong: %+v", active)
	}

	// "all" shows both.
	all, err := l.ListDecisions(ctx, project, "all", "", 50)
	if err != nil || len(all) != 2 {
		t.Fatalf("ListDecisions(all): %v count=%d", err, len(all))
	}

	// Events.
	if _, err := l.RecordEvent(ctx, project, Event{
		Type: "claude_md_change", Summary: "CLAUDE.md changed", Path: "/x/CLAUDE.md", Hash: "abc",
	}); err != nil {
		t.Fatalf("RecordEvent: %v", err)
	}
	events, err := l.ListEvents(ctx, project, "claude_md_change", 10)
	if err != nil || len(events) != 1 {
		t.Fatalf("ListEvents: %v count=%d", err, len(events))
	}

	// Stats.
	stats, err := l.Stats(ctx, project)
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.DecisionsTotal != 2 || stats.DecisionsActive != 1 || stats.EventsTotal != 1 {
		t.Fatalf("unexpected stats: %+v", stats)
	}
}

func TestSupersedeMissingFails(t *testing.T) {
	ctx := context.Background()
	l := New(t.TempDir())
	_, err := l.RecordDecision(ctx, "p", Decision{
		Title: "x", Rationale: "y", Supersedes: 999,
	})
	if err == nil {
		t.Fatal("expected error superseding a non-existent decision, got nil")
	}
	// The failed write must not have leaked a decision or bumped the sequence.
	all, err := l.ListDecisions(ctx, "p", "all", "", 50)
	if err != nil {
		t.Fatalf("ListDecisions: %v", err)
	}
	if len(all) != 0 {
		t.Fatalf("expected no decisions after failed supersede, got %d", len(all))
	}
}

func TestSeqCorruptionIsFatal(t *testing.T) {
	ctx := context.Background()
	l := New(t.TempDir())
	if _, err := l.RecordDecision(ctx, "p", Decision{Title: "a", Rationale: "b"}); err != nil {
		t.Fatalf("seed: %v", err)
	}
	// Corrupt the sequence value directly.
	if err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		return execAll(ctx, db, kvPair{seqKey("p", "decision"), []byte("not-a-number")})
	}); err != nil {
		t.Fatalf("corrupt seq: %v", err)
	}
	if _, err := l.RecordDecision(ctx, "p", Decision{Title: "c", Rationale: "d"}); err == nil {
		t.Fatal("expected fatal error on corrupted sequence, got nil")
	}
}

func TestPaginationBeyondBatch(t *testing.T) {
	old := scanBatch
	scanBatch = 10
	defer func() { scanBatch = old }()

	ctx := context.Background()
	l := New(t.TempDir())
	const n = 25 // > 2 pages
	for i := range n {
		if _, err := l.RecordDecision(ctx, "p", Decision{
			Title: "d", Rationale: "r", Tags: "t",
		}); err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
	}
	stats, err := l.Stats(ctx, "p")
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	if stats.DecisionsTotal != n {
		t.Fatalf("pagination lost records: got %d want %d", stats.DecisionsTotal, n)
	}
	// Newest-first ordering must survive pagination.
	all, err := l.ListDecisions(ctx, "p", "all", "", 1000)
	if err != nil || len(all) != n {
		t.Fatalf("ListDecisions: %v count=%d", err, len(all))
	}
	if all[0].ID != n {
		t.Fatalf("expected newest id %d first, got %d", n, all[0].ID)
	}
}

func TestVerifyDecision(t *testing.T) {
	ctx := context.Background()
	l := New(t.TempDir())
	const project = "demo"

	d, err := l.RecordDecision(ctx, project, Decision{
		Title:     "Store the ledger in immudb",
		Rationale: "Tamper-evidence is the whole point.",
		Tags:      "storage",
	})
	if err != nil {
		t.Fatalf("RecordDecision: %v", err)
	}
	// A few more records so the verified read spans multiple transactions.
	for range 3 {
		if _, err := l.RecordEvent(ctx, project, Event{Type: "note", Summary: "tick"}); err != nil {
			t.Fatalf("RecordEvent: %v", err)
		}
	}

	res, err := l.VerifyDecision(ctx, project, d.ID, "", 0)
	if err != nil {
		t.Fatalf("VerifyDecision: %v", err)
	}
	if !res.Verified {
		t.Fatalf("expected verified=true, got: %+v", res)
	}
	if res.RootHash == "" || res.TxID == 0 {
		t.Fatalf("missing proof anchor: %+v", res)
	}

	// Pinning the correct current root still verifies.
	okRes, err := l.VerifyDecision(ctx, project, d.ID, res.RootHash, res.RootTxID)
	if err != nil || !okRes.Verified {
		t.Fatalf("expected verified with matching expected root: %+v err=%v", okRes, err)
	}

	// Pinning a wrong root must fail (models a replaced data directory).
	badRes, err := l.VerifyDecision(ctx, project, d.ID, strings.Repeat("de", 32), res.RootTxID)
	if err != nil {
		t.Fatalf("VerifyDecision(expected mismatch): %v", err)
	}
	if badRes.Verified {
		t.Fatalf("expected verified=false for mismatched root, got true")
	}
}

// An anchor recorded out of band must keep verifying after later writes advance
// the root. The previous implementation compared expectedRoot for equality with
// the *current* root, so any subsequent append turned the pin into a false
// "the data directory may have been replaced" alarm on a healthy ledger.
func TestVerifyDecisionWithPinnedOlderAnchor(t *testing.T) {
	ctx := context.Background()
	l := New(t.TempDir())
	const project = "demo"

	d, err := l.RecordDecision(ctx, project, Decision{Title: "anchor me", Rationale: "why"})
	if err != nil {
		t.Fatalf("RecordDecision: %v", err)
	}

	// Take the anchor the way ledger_status hands it to the user.
	anchorTx, anchorHash, err := l.CurrentRoot(ctx)
	if err != nil {
		t.Fatalf("CurrentRoot: %v", err)
	}

	// Ordinary subsequent activity advances the root.
	for range 3 {
		if _, err := l.RecordEvent(ctx, project, Event{Type: "note", Summary: "tick"}); err != nil {
			t.Fatalf("RecordEvent: %v", err)
		}
	}

	res, err := l.VerifyDecision(ctx, project, d.ID, anchorHash, anchorTx)
	if err != nil {
		t.Fatalf("VerifyDecision(pinned anchor): %v", err)
	}
	if !res.Verified {
		t.Fatalf("a pinned older anchor must still verify on an untampered ledger: %+v", res)
	}
	if res.RootTxID <= anchorTx {
		t.Fatalf("expected the root to have advanced past the anchor: root=%d anchor=%d", res.RootTxID, anchorTx)
	}

	// A wrong hash at a real tx id is a genuine replacement signal.
	if bad, err := l.VerifyDecision(ctx, project, d.ID, strings.Repeat("ab", 32), anchorTx); err != nil {
		t.Fatalf("VerifyDecision(wrong anchor hash): %v", err)
	} else if bad.Verified {
		t.Fatal("expected verified=false for a wrong hash at a valid anchor tx")
	}

	// An anchor newer than the ledger means it was replaced or truncated.
	if bad, err := l.VerifyDecision(ctx, project, d.ID, anchorHash, res.RootTxID+100); err != nil {
		t.Fatalf("VerifyDecision(future anchor): %v", err)
	} else if bad.Verified {
		t.Fatal("expected verified=false for an anchor tx beyond the ledger")
	}
}

// The field caps admit records well above immudb's 4 KB default MaxValueLen, so
// the store must be created with room for them.
func TestRecordDecisionWithLargeRationale(t *testing.T) {
	ctx := context.Background()
	l := New(t.TempDir())

	rationale := strings.Repeat("a very ordinary ADR paragraph. ", 170) // ~5 KB
	if len(rationale) < 5000 {
		t.Fatalf("test setup: rationale is only %d bytes", len(rationale))
	}

	d, err := l.RecordDecision(ctx, "p", Decision{Title: "big", Rationale: rationale})
	if err != nil {
		t.Fatalf("RecordDecision with a %d-byte rationale: %v", len(rationale), err)
	}
	got, err := l.GetDecision(ctx, "p", d.ID)
	if err != nil || got == nil {
		t.Fatalf("GetDecision: %v (nil=%v)", err, got == nil)
	}
	if got.Rationale != rationale {
		t.Fatalf("rationale round-trip mismatch: got %d bytes want %d", len(got.Rationale), len(rationale))
	}
}

// A record that cannot be read back must surface as an error, not vanish from
// every read path while the remaining records are presented as a complete
// ledger.
func TestCorruptedRecordIsReported(t *testing.T) {
	ctx := context.Background()
	l := New(t.TempDir())

	if _, err := l.RecordDecision(ctx, "p", Decision{Title: "a", Rationale: "b"}); err != nil {
		t.Fatalf("seed decision: %v", err)
	}
	if _, err := l.RecordEvent(ctx, "p", Event{Type: "note", Summary: "s"}); err != nil {
		t.Fatalf("seed event: %v", err)
	}

	badDecision := decisionKey("p", 1)
	badEvent := eventKey("p", 1)
	if err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		return execAll(ctx, db,
			kvPair{badDecision, []byte("{not json")},
			kvPair{badEvent, []byte("{not json")},
		)
	}); err != nil {
		t.Fatalf("corrupt records: %v", err)
	}

	if _, err := l.ListDecisions(ctx, "p", "all", "", 50); err == nil {
		t.Fatal("ListDecisions silently dropped a corrupted decision")
	} else if !strings.Contains(err.Error(), badDecision) {
		t.Fatalf("error should name the corrupted key, got: %v", err)
	}

	if _, err := l.Stats(ctx, "p"); err == nil {
		t.Fatal("Stats silently dropped a corrupted decision")
	}

	if _, err := l.ListEvents(ctx, "p", "", 50); err == nil {
		t.Fatal("ListEvents silently dropped a corrupted event")
	} else if !strings.Contains(err.Error(), badEvent) {
		t.Fatalf("error should name the corrupted key, got: %v", err)
	}
}
