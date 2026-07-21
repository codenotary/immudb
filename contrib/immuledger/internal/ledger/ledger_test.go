package ledger

import (
	"context"
	"testing"
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

	res, err := l.VerifyDecision(ctx, project, d.ID)
	if err != nil {
		t.Fatalf("VerifyDecision: %v", err)
	}
	if !res.Verified {
		t.Fatalf("expected verified=true, got: %+v", res)
	}
	if res.RootHash == "" || res.TxID == 0 {
		t.Fatalf("missing proof anchor: %+v", res)
	}
}
