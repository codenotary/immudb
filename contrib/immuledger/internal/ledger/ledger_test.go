package ledger

import (
	"context"
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

	res, err := l.VerifyDecision(ctx, project, d.ID, "")
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
	okRes, err := l.VerifyDecision(ctx, project, d.ID, res.RootHash)
	if err != nil || !okRes.Verified {
		t.Fatalf("expected verified with matching expected root: %+v err=%v", okRes, err)
	}

	// Pinning a wrong root must fail (models a replaced data directory).
	badRes, err := l.VerifyDecision(ctx, project, d.ID, "deadbeef")
	if err != nil {
		t.Fatalf("VerifyDecision(expected mismatch): %v", err)
	}
	if badRes.Verified {
		t.Fatalf("expected verified=false for mismatched root, got true")
	}
}
