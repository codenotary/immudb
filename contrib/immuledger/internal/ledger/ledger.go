// Package ledger is the immudb-backed data layer for the immuledger plugin.
//
// immudb is embedded in-process (via pkg/database): there is no immudb server
// or container. Because immudb does not lock its data directory, and the MCP
// server and hook processes are separate, every operation acquires a file lock,
// then opens the store, performs the work, and closes it again ("open per
// operation"). Ledger traffic is infrequent, so this is inexpensive and keeps
// the tamper-evident guarantees of immudb without a long-lived process.
//
// Decisions and events are stored as key-value entries (JSON values) under
// per-project key prefixes, so one data directory serves many repositories and
// every record is individually verifiable (see verify.go).
package ledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/store"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
)

const (
	dbName        = "immuledger"
	lockTimeout   = 30 * time.Second
	maxScan       = 1000
	statusActive  = "active"
	statusRetired = "superseded"
)

// Decision is a recorded architecture/design decision (an ADR).
type Decision struct {
	ID           int    `json:"id"`
	Project      string `json:"project"`
	Title        string `json:"title"`
	Rationale    string `json:"rationale"`
	Alternatives string `json:"alternatives,omitempty"`
	Tags         string `json:"tags,omitempty"`
	Status       string `json:"status"`
	Supersedes   int    `json:"supersedes,omitempty"`
	SupersededBy int    `json:"superseded_by,omitempty"`
	Author       string `json:"author,omitempty"`
	CreatedAt    string `json:"created_at"`
}

// Event is a generic ledger event (e.g. a CLAUDE.md change).
type Event struct {
	ID        int    `json:"id"`
	Project   string `json:"project"`
	Type      string `json:"type"`
	Summary   string `json:"summary"`
	Payload   string `json:"payload,omitempty"`
	Path      string `json:"path,omitempty"`
	Hash      string `json:"hash,omitempty"`
	CreatedAt string `json:"created_at"`
}

// Stats summarizes a project's ledger.
type Stats struct {
	Project         string `json:"project"`
	DecisionsTotal  int    `json:"decisions_total"`
	DecisionsActive int    `json:"decisions_active"`
	EventsTotal     int    `json:"events_total"`
}

// Ledger opens the embedded immudb data directory on demand.
type Ledger struct {
	dataDir string
}

// New returns a Ledger rooted at dataDir. If dataDir is empty, DefaultDataDir
// is used.
func New(dataDir string) *Ledger {
	if strings.TrimSpace(dataDir) == "" {
		dataDir = DefaultDataDir()
	}
	return &Ledger{dataDir: dataDir}
}

// DefaultDataDir resolves the ledger location: IMMULEDGER_DATA_DIR, else
// ~/.immuledger, else ./.immuledger.
func DefaultDataDir() string {
	if d := strings.TrimSpace(os.Getenv("IMMULEDGER_DATA_DIR")); d != "" {
		return d
	}
	if home, err := os.UserHomeDir(); err == nil && home != "" {
		return filepath.Join(home, ".immuledger")
	}
	return ".immuledger"
}

// DataDir returns the resolved data directory.
func (l *Ledger) DataDir() string { return l.dataDir }

// withDB acquires the cross-process lock, opens (or creates) the store, runs fn,
// and closes the store. immudb's logger is sent to stderr so it never corrupts
// the MCP stdout stream or a hook's stdout digest.
func (l *Ledger) withDB(ctx context.Context, fn func(ctx context.Context, db database.DB) error) (err error) {
	if err = os.MkdirAll(l.dataDir, 0o755); err != nil {
		return fmt.Errorf("create data dir: %w", err)
	}

	release, err := acquireLock(filepath.Join(l.dataDir, ".immuledger.lock"), lockTimeout)
	if err != nil {
		return err
	}
	defer release()

	log := logger.NewSimpleLoggerWithLevel("immuledger", os.Stderr, logger.LogError)
	opts := database.DefaultOptions().WithDBRootPath(l.dataDir)

	var db database.DB
	if _, statErr := os.Stat(filepath.Join(l.dataDir, dbName)); errors.Is(statErr, os.ErrNotExist) {
		db, err = database.NewDB(dbName, nil, opts, log)
	} else {
		db, err = database.OpenDB(dbName, nil, opts, log)
	}
	if err != nil {
		return fmt.Errorf("open ledger: %w", err)
	}
	defer func() {
		if cerr := db.Close(); err == nil {
			err = cerr
		}
	}()

	return fn(ctx, db)
}

// --- key helpers ----------------------------------------------------------

func decisionKey(project string, id int) string {
	return fmt.Sprintf("immuledger/%s/decision/%012d", project, id)
}

func decisionPrefix(project string) string {
	return fmt.Sprintf("immuledger/%s/decision/", project)
}

func eventKey(project string, id int) string {
	return fmt.Sprintf("immuledger/%s/event/%012d", project, id)
}

func eventPrefix(project string) string {
	return fmt.Sprintf("immuledger/%s/event/", project)
}

func seqKey(project, kind string) string {
	return fmt.Sprintf("immuledger/%s/seq/%s", project, kind)
}

// --- low-level KV ---------------------------------------------------------

func kvSet(ctx context.Context, db database.DB, key string, val []byte) error {
	_, err := db.Set(ctx, &schema.SetRequest{
		KVs: []*schema.KeyValue{{Key: []byte(key), Value: val}},
	})
	return err
}

func kvGet(ctx context.Context, db database.DB, key string) ([]byte, bool, error) {
	e, err := db.Get(ctx, &schema.KeyRequest{Key: []byte(key)})
	if errors.Is(err, store.ErrKeyNotFound) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, err
	}
	return e.Value, true, nil
}

func kvScan(ctx context.Context, db database.DB, prefix string, limit uint64) ([]*schema.Entry, error) {
	entries, err := db.Scan(ctx, &schema.ScanRequest{Prefix: []byte(prefix), Limit: limit})
	if err != nil {
		return nil, err
	}
	return entries.Entries, nil
}

func nextSeq(ctx context.Context, db database.DB, project, kind string) (int, error) {
	cur := 0
	if v, ok, err := kvGet(ctx, db, seqKey(project, kind)); err != nil {
		return 0, err
	} else if ok {
		cur, _ = strconv.Atoi(strings.TrimSpace(string(v)))
	}
	n := cur + 1
	if err := kvSet(ctx, db, seqKey(project, kind), []byte(strconv.Itoa(n))); err != nil {
		return 0, err
	}
	return n, nil
}

// --- decisions ------------------------------------------------------------

// RecordDecision appends a decision and, if supersedes > 0, marks the prior
// decision superseded (its earlier version is retained in immudb history).
func (l *Ledger) RecordDecision(ctx context.Context, project string, d Decision) (Decision, error) {
	var out Decision
	err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		id, err := nextSeq(ctx, db, project, "decision")
		if err != nil {
			return err
		}
		d.ID = id
		d.Project = project
		d.Status = statusActive
		if d.CreatedAt == "" {
			d.CreatedAt = nowRFC3339()
		}
		body, err := json.Marshal(d)
		if err != nil {
			return err
		}
		if err := kvSet(ctx, db, decisionKey(project, id), body); err != nil {
			return err
		}

		if d.Supersedes > 0 {
			if prev, ok, err := kvGet(ctx, db, decisionKey(project, d.Supersedes)); err != nil {
				return err
			} else if ok {
				var od Decision
				if json.Unmarshal(prev, &od) == nil {
					od.Status = statusRetired
					od.SupersededBy = id
					if nb, err := json.Marshal(od); err == nil {
						if err := kvSet(ctx, db, decisionKey(project, d.Supersedes), nb); err != nil {
							return err
						}
					}
				}
			}
		}
		out = d
		return nil
	})
	return out, err
}

// GetDecision returns a single decision by id.
func (l *Ledger) GetDecision(ctx context.Context, project string, id int) (*Decision, error) {
	var found *Decision
	err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		b, ok, err := kvGet(ctx, db, decisionKey(project, id))
		if err != nil || !ok {
			return err
		}
		var d Decision
		if err := json.Unmarshal(b, &d); err != nil {
			return err
		}
		found = &d
		return nil
	})
	return found, err
}

// ListDecisions returns decisions for a project (newest first). status is
// "active" (default), "superseded", or "all". tag filters by substring.
func (l *Ledger) ListDecisions(ctx context.Context, project, status, tag string, limit int) ([]Decision, error) {
	all, err := l.loadDecisions(ctx, project)
	if err != nil {
		return nil, err
	}
	status = strings.ToLower(strings.TrimSpace(status))
	tag = strings.ToLower(strings.TrimSpace(tag))
	out := make([]Decision, 0, len(all))
	for _, d := range all {
		if status != "" && status != "all" && !strings.EqualFold(d.Status, status) {
			continue
		}
		if tag != "" && !strings.Contains(strings.ToLower(d.Tags), tag) {
			continue
		}
		out = append(out, d)
	}
	return capDecisions(out, limit), nil
}

// SearchDecisions matches a query (case-insensitive substring) against title,
// rationale, and tags.
func (l *Ledger) SearchDecisions(ctx context.Context, project, query string, limit int) ([]Decision, error) {
	all, err := l.loadDecisions(ctx, project)
	if err != nil {
		return nil, err
	}
	q := strings.ToLower(strings.TrimSpace(query))
	out := make([]Decision, 0, len(all))
	for _, d := range all {
		hay := strings.ToLower(d.Title + "\n" + d.Rationale + "\n" + d.Tags)
		if q == "" || strings.Contains(hay, q) {
			out = append(out, d)
		}
	}
	return capDecisions(out, limit), nil
}

func (l *Ledger) loadDecisions(ctx context.Context, project string) ([]Decision, error) {
	var out []Decision
	err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		entries, err := kvScan(ctx, db, decisionPrefix(project), maxScan)
		if err != nil {
			return err
		}
		for _, e := range entries {
			var d Decision
			if json.Unmarshal(e.Value, &d) == nil && d.ID != 0 {
				out = append(out, d)
			}
		}
		return nil
	})
	// newest first
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	return out, err
}

// Stats returns per-project counts.
func (l *Ledger) Stats(ctx context.Context, project string) (Stats, error) {
	s := Stats{Project: project}
	err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		dec, err := kvScan(ctx, db, decisionPrefix(project), maxScan)
		if err != nil {
			return err
		}
		for _, e := range dec {
			var d Decision
			if json.Unmarshal(e.Value, &d) == nil && d.ID != 0 {
				s.DecisionsTotal++
				if strings.EqualFold(d.Status, statusActive) {
					s.DecisionsActive++
				}
			}
		}
		ev, err := kvScan(ctx, db, eventPrefix(project), maxScan)
		if err != nil {
			return err
		}
		for _, e := range ev {
			var evt Event
			if json.Unmarshal(e.Value, &evt) == nil && evt.ID != 0 {
				s.EventsTotal++
			}
		}
		return nil
	})
	return s, err
}

// --- events ---------------------------------------------------------------

// RecordEvent appends a generic event to the ledger.
func (l *Ledger) RecordEvent(ctx context.Context, project string, e Event) (Event, error) {
	var out Event
	err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		id, err := nextSeq(ctx, db, project, "event")
		if err != nil {
			return err
		}
		e.ID = id
		e.Project = project
		if e.CreatedAt == "" {
			e.CreatedAt = nowRFC3339()
		}
		body, err := json.Marshal(e)
		if err != nil {
			return err
		}
		if err := kvSet(ctx, db, eventKey(project, id), body); err != nil {
			return err
		}
		out = e
		return nil
	})
	return out, err
}

// ListEvents returns events for a project (newest first), optionally filtered
// by type.
func (l *Ledger) ListEvents(ctx context.Context, project, eventType string, limit int) ([]Event, error) {
	var out []Event
	err := l.withDB(ctx, func(ctx context.Context, db database.DB) error {
		entries, err := kvScan(ctx, db, eventPrefix(project), maxScan)
		if err != nil {
			return err
		}
		for _, e := range entries {
			var evt Event
			if json.Unmarshal(e.Value, &evt) == nil && evt.ID != 0 {
				if eventType == "" || strings.EqualFold(evt.Type, eventType) {
					out = append(out, evt)
				}
			}
		}
		return nil
	})
	sort.Slice(out, func(i, j int) bool { return out[i].ID > out[j].ID })
	if limit > 0 && len(out) > limit {
		out = out[:limit]
	}
	return out, err
}

// EnsureReady opens (creating if needed) the ledger, confirming connectivity.
func (l *Ledger) EnsureReady(ctx context.Context) error {
	return l.withDB(ctx, func(ctx context.Context, db database.DB) error { return nil })
}

// --- helpers --------------------------------------------------------------

func capDecisions(d []Decision, limit int) []Decision {
	if limit > 0 && len(d) > limit {
		return d[:limit]
	}
	return d
}

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}
