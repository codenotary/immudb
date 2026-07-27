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

// Package mcp registers the immuledger MCP tools and delegates them to the
// embedded-immudb ledger. The tool names match the reference design so the
// slash-command prompts work unchanged.
package mcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/mark3labs/mcp-go/mcp"
	"github.com/mark3labs/mcp-go/server"

	"github.com/codenotary/immudb/contrib/immuledger/internal/ledger"
	"github.com/codenotary/immudb/contrib/immuledger/internal/project"
	"github.com/codenotary/immudb/embedded/store"
)

// Version is reported to MCP clients.
const Version = "0.1.0"

// NewServer builds the MCP server backed by the given ledger.
func NewServer(l *ledger.Ledger) *server.MCPServer {
	s := server.NewMCPServer("immuledger", Version,
		server.WithToolCapabilities(true),
	)
	register(s, l)
	return s
}

func register(s *server.MCPServer, l *ledger.Ledger) {
	proj := func(req mcp.CallToolRequest) string {
		if p := strings.TrimSpace(req.GetString("project", "")); p != "" {
			return project.Sanitize(p)
		}
		return project.Name("")
	}

	s.AddTool(mcp.NewTool("init_ledger",
		mcp.WithDescription("Create/open the immudb-backed ledger and confirm connectivity. Safe to run repeatedly."),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		if err := l.EnsureReady(ctx); err != nil {
			return errResult(err), nil
		}
		return ok(map[string]any{"ok": true, "data_dir": l.DataDir(), "message": "Ledger ready."})
	})

	s.AddTool(mcp.NewTool("record_decision",
		mcp.WithDescription("Append a decision (an ADR) to the immutable ledger for this project. Use whenever the user makes or confirms a choice worth remembering."),
		mcp.WithString("title", mcp.Required(), mcp.Description("One-line statement of the decision.")),
		mcp.WithString("rationale", mcp.Required(), mcp.Description("Why this was chosen — the durable part that must survive context loss.")),
		mcp.WithString("alternatives", mcp.Description("Options considered and why they were rejected.")),
		mcp.WithString("tags", mcp.Description("Comma-separated keywords for later retrieval.")),
		mcp.WithString("author", mcp.Description("Who decided (optional).")),
		mcp.WithNumber("supersedes", mcp.Description("Id of a prior decision this replaces; that one is marked superseded but retained in history.")),
		mcp.WithString("project", mcp.Description("Override the auto-detected project name.")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		title, err := req.RequireString("title")
		if err != nil {
			return mcp.NewToolResultError("title is required"), nil
		}
		rationale, err := req.RequireString("rationale")
		if err != nil {
			return mcp.NewToolResultError("rationale is required"), nil
		}
		d, err := l.RecordDecision(ctx, proj(req), ledger.Decision{
			Title:        title,
			Rationale:    rationale,
			Alternatives: req.GetString("alternatives", ""),
			Tags:         req.GetString("tags", ""),
			Author:       req.GetString("author", ""),
			Supersedes:   req.GetInt("supersedes", 0),
		})
		if err != nil {
			return errResult(err), nil
		}
		return ok(map[string]any{"ok": true, "id": d.ID, "project": d.Project, "title": d.Title, "status": d.Status, "supersedes": d.Supersedes})
	})

	s.AddTool(mcp.NewTool("list_decisions",
		mcp.WithDescription("List decisions for this project (newest first)."),
		mcp.WithString("status", mcp.Description("active (default), superseded, or all."), mcp.DefaultString("active")),
		mcp.WithString("tag", mcp.Description("Only decisions whose tags contain this keyword.")),
		mcp.WithNumber("limit", mcp.Description("Maximum number to return (default 50).")),
		mcp.WithString("project", mcp.Description("Override the auto-detected project name.")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		p := proj(req)
		rows, err := l.ListDecisions(ctx, p, req.GetString("status", "active"), req.GetString("tag", ""), req.GetInt("limit", 50))
		if err != nil {
			return errResult(err), nil
		}
		return ok(map[string]any{"ok": true, "project": p, "count": len(rows), "decisions": rows})
	})

	s.AddTool(mcp.NewTool("get_decision",
		mcp.WithDescription("Fetch the full detail of a single decision by id (rationale and alternatives included)."),
		mcp.WithNumber("id", mcp.Required(), mcp.Description("Decision id.")),
		mcp.WithString("project", mcp.Description("Override the auto-detected project name.")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id := req.GetInt("id", 0)
		if id <= 0 {
			return mcp.NewToolResultError("id is required"), nil
		}
		d, err := l.GetDecision(ctx, proj(req), id)
		if err != nil {
			return errResult(err), nil
		}
		if d == nil {
			return ok(map[string]any{"ok": true, "found": false, "id": id})
		}
		return ok(map[string]any{"ok": true, "found": true, "decision": d})
	})

	s.AddTool(mcp.NewTool("search_decisions",
		mcp.WithDescription("Search decision titles, rationales, and tags for this project."),
		mcp.WithString("query", mcp.Required(), mcp.Description("Search term.")),
		mcp.WithNumber("limit", mcp.Description("Maximum number to return (default 25).")),
		mcp.WithString("project", mcp.Description("Override the auto-detected project name.")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		query, err := req.RequireString("query")
		if err != nil {
			return mcp.NewToolResultError("query is required"), nil
		}
		p := proj(req)
		rows, err := l.SearchDecisions(ctx, p, query, req.GetInt("limit", 25))
		if err != nil {
			return errResult(err), nil
		}
		return ok(map[string]any{"ok": true, "project": p, "query": query, "count": len(rows), "decisions": rows})
	})

	s.AddTool(mcp.NewTool("check_compliance",
		mcp.WithDescription("Return the project's active decisions (and any that overlap the task description) so the current plan or change can be checked against them BEFORE implementing. The caller compares the proposed work and flags conflicts."),
		mcp.WithString("task_description", mcp.Description("What you are about to do.")),
		mcp.WithNumber("limit", mcp.Description("Maximum active decisions to return (default 50).")),
		mcp.WithString("project", mcp.Description("Override the auto-detected project name.")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		p := proj(req)
		limit := req.GetInt("limit", 50)
		active, err := l.ListDecisions(ctx, p, "active", "", limit)
		if err != nil {
			return errResult(err), nil
		}
		task := req.GetString("task_description", "")
		relevant := relevantDecisions(ctx, l, p, task)
		return ok(map[string]any{
			"ok":                 true,
			"project":            p,
			"task_description":   task,
			"active_decisions":   active,
			"relevant_decisions": relevant,
			"instruction": "Compare the proposed work against active_decisions (and the relevant_decisions surfaced by keyword). " +
				"Explicitly call out any decision the work would violate before proceeding.",
		})
	})

	s.AddTool(mcp.NewTool("record_event",
		mcp.WithDescription("Append a generic event to the ledger (e.g. a CLAUDE.md change, a milestone, an approval)."),
		mcp.WithString("event_type", mcp.Required(), mcp.Description("Short label, e.g. 'claude_md_change' or 'note'.")),
		mcp.WithString("summary", mcp.Required(), mcp.Description("One-line summary.")),
		mcp.WithString("payload", mcp.Description("Optional detail.")),
		mcp.WithString("path", mcp.Description("Optional file path.")),
		mcp.WithString("hash", mcp.Description("Optional sha256 of the referenced file.")),
		mcp.WithString("project", mcp.Description("Override the auto-detected project name.")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		et, err := req.RequireString("event_type")
		if err != nil {
			return mcp.NewToolResultError("event_type is required"), nil
		}
		summary, err := req.RequireString("summary")
		if err != nil {
			return mcp.NewToolResultError("summary is required"), nil
		}
		e, err := l.RecordEvent(ctx, proj(req), ledger.Event{
			Type:    et,
			Summary: summary,
			Payload: req.GetString("payload", ""),
			Path:    req.GetString("path", ""),
			Hash:    req.GetString("hash", ""),
		})
		if err != nil {
			return errResult(err), nil
		}
		return ok(map[string]any{"ok": true, "id": e.ID, "project": e.Project, "event_type": e.Type})
	})

	s.AddTool(mcp.NewTool("list_events",
		mcp.WithDescription("List recorded events for this project (newest first). Filter by event_type if given."),
		mcp.WithString("event_type", mcp.Description("e.g. 'claude_md_change'.")),
		mcp.WithNumber("limit", mcp.Description("Maximum number to return (default 50).")),
		mcp.WithString("project", mcp.Description("Override the auto-detected project name.")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		p := proj(req)
		rows, err := l.ListEvents(ctx, p, req.GetString("event_type", ""), req.GetInt("limit", 50))
		if err != nil {
			return errResult(err), nil
		}
		return ok(map[string]any{"ok": true, "project": p, "count": len(rows), "events": rows})
	})

	s.AddTool(mcp.NewTool("ledger_status",
		mcp.WithDescription("Report this project's ledger counts plus the ledger's current tamper-evident root (tx id + hash)."),
		mcp.WithString("project", mcp.Description("Override the auto-detected project name.")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		p := proj(req)
		stats, err := l.Stats(ctx, p)
		if err != nil {
			return errResult(err), nil
		}
		rootTx, rootHash, err := l.CurrentRoot(ctx)
		if err != nil {
			return errResult(err), nil
		}
		return ok(map[string]any{
			"ok":               true,
			"project":          stats.Project,
			"decisions_total":  stats.DecisionsTotal,
			"decisions_active": stats.DecisionsActive,
			"events_total":     stats.EventsTotal,
			"root_tx_id":       rootTx,
			"root_hash":        rootHash,
			"integrity_note": "immudb stores every version of every record in a tamper-evident Merkle tree. " +
				"Use verify_decision to cryptographically prove a specific decision is included and consistent with this root — all in-process, no server.",
		})
	})

	s.AddTool(mcp.NewTool("verify_decision",
		mcp.WithDescription("Cryptographically verify a decision in-process: checks its inclusion proof and a dual proof linking it to the ledger's current root. Returns verified true/false and the root anchor. Optionally pass an externally recorded anchor as expected_root plus expected_root_tx_id (both are returned together by ledger_status) to also prove the current root is a consistent extension of that anchor, which detects a replaced data directory."),
		mcp.WithNumber("id", mcp.Required(), mcp.Description("Decision id to verify.")),
		mcp.WithString("expected_root", mcp.Description("Optional externally recorded root hash (hex) to pin as an anchor.")),
		mcp.WithNumber("expected_root_tx_id", mcp.Description("Transaction id the expected_root was recorded at. Required for an anchor taken before later writes: without it only equality with the current root can be checked, which no longer holds once anything has been appended.")),
		mcp.WithString("project", mcp.Description("Override the auto-detected project name.")),
	), func(ctx context.Context, req mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		id := req.GetInt("id", 0)
		if id <= 0 {
			return mcp.NewToolResultError("id is required"), nil
		}
		anchorTx := req.GetInt("expected_root_tx_id", 0)
		if anchorTx < 0 {
			return mcp.NewToolResultError("expected_root_tx_id must not be negative"), nil
		}
		res, err := l.VerifyDecision(ctx, proj(req), id, req.GetString("expected_root", ""), uint64(anchorTx))
		if err != nil {
			return errResult(err), nil
		}
		return ok(map[string]any{"ok": true, "verified": res.Verified, "decision_id": res.DecisionID,
			"tx_id": res.TxID, "root_tx_id": res.RootTxID, "root_hash": res.RootHash, "detail": res.Detail})
	})
}

// maxKeywords bounds how many words of a task description are searched for.
const maxKeywords = 12

// taskKeywords extracts the words a task description is searched by, in first
// appearance order and deduplicated.
//
// The order matters: ranging over a map to pick the first maxKeywords words
// would hand back a different set on every call, because Go randomizes map
// iteration order. That made check_compliance non-deterministic — the same
// question could surface the decision the work violates on one run and omit it
// on the next, which is precisely the failure the tool exists to prevent.
func taskKeywords(task string, max int) []string {
	seen := map[string]bool{}
	var out []string
	for _, w := range strings.FieldsFunc(task, func(r rune) bool {
		return r == ' ' || r == ',' || r == '.' || r == '\n'
	}) {
		w = strings.ToLower(w)
		if len(w) <= 3 || seen[w] {
			continue
		}
		seen[w] = true
		out = append(out, w)
		if len(out) >= max {
			break
		}
	}
	return out
}

// relevantDecisions surfaces active/superseded decisions whose text overlaps the
// task description by keyword (best-effort; never fatal).
func relevantDecisions(ctx context.Context, l *ledger.Ledger, project, task string) []ledger.Decision {
	if strings.TrimSpace(task) == "" {
		return nil
	}
	seen := map[int]bool{}
	var out []ledger.Decision
	for _, w := range taskKeywords(task, maxKeywords) {
		rows, err := l.SearchDecisions(ctx, project, w, 10)
		if err != nil {
			continue
		}
		for _, d := range rows {
			if !seen[d.ID] {
				seen[d.ID] = true
				out = append(out, d)
			}
		}
	}
	return out
}

func ok(payload map[string]any) (*mcp.CallToolResult, error) {
	b, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return mcp.NewToolResultError(fmt.Sprintf("marshal result: %v", err)), nil
	}
	return mcp.NewToolResultText(string(b)), nil
}

// errResult reports a ledger failure. It must go back as a tool *error*: with
// only "ok": false in an otherwise successful result the caller is being told a
// decision was recorded when nothing was written, which defeats the point of an
// audit trail. NewToolResultError sets IsError on the result; NewToolResultText
// does not.
func errResult(err error) *mcp.CallToolResult {
	payload := map[string]any{
		"ok":    false,
		"error": err.Error(),
		"hint":  errHint(err),
	}
	b, merr := json.MarshalIndent(payload, "", "  ")
	if merr != nil {
		return mcp.NewToolResultError(err.Error())
	}
	return mcp.NewToolResultError(string(b))
}

// errHint gives the caller something actionable. Blaming connectivity for every
// failure is worse than saying nothing — an oversized record or a corrupted
// entry has nothing to do with IMMULEDGER_DATA_DIR being writable.
func errHint(err error) string {
	msg := err.Error()
	switch {
	case errors.Is(err, store.ErrMaxValueLenExceeded):
		return "The record exceeds this ledger's per-value size limit. Shorten the rationale, alternatives or payload, or start a fresh IMMULEDGER_DATA_DIR (the limit is fixed when a ledger is created)."
	case strings.Contains(msg, "could not acquire ledger lock"):
		return "Another immuledger process (an MCP server, a SessionStart digest, or a parallel session) is holding the ledger lock. Retry shortly; if it persists, remove a stale .immuledger.lock in IMMULEDGER_DATA_DIR."
	case strings.Contains(msg, "corrupted"):
		return "The ledger contains a record that cannot be read back. This is reported rather than skipped so a missing record is never presented as a complete history — inspect IMMULEDGER_DATA_DIR and restore from a backup."
	default:
		return "Could not complete the ledger operation. Check IMMULEDGER_DATA_DIR exists and is writable, and that no other immuledger process is stuck holding the lock."
	}
}
