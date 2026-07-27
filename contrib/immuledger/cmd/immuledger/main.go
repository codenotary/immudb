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

// Command immuledger is the single binary behind the immuledger Claude Code
// plugin. It has three modes:
//
//	immuledger serve            run the MCP server over stdio (the plugin's mcpServer)
//	immuledger digest           SessionStart hook: print a digest of active decisions
//	immuledger record-claudemd  PostToolUse hook: record CLAUDE.md/.claude changes
//
// All modes embed immudb in-process (no server, no container). The hook modes
// never block a session — but they are not silent about failure: a
// claude_md_change that could not be written is reported on stderr, appended to
// a failure log, and signalled with a non-zero exit, because an unrecorded event
// is indistinguishable from an event that never happened when the ledger is read
// back later.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	mcpserver "github.com/mark3labs/mcp-go/server"

	"github.com/codenotary/immudb/contrib/immuledger/internal/ledger"
	"github.com/codenotary/immudb/contrib/immuledger/internal/mcp"
	"github.com/codenotary/immudb/contrib/immuledger/internal/project"
)

func main() {
	mode := "serve"
	if len(os.Args) > 1 {
		mode = os.Args[1]
	}

	switch mode {
	case "serve":
		if err := serve(); err != nil {
			fmt.Fprintln(os.Stderr, "immuledger serve:", err)
			os.Exit(1)
		}
	case "digest":
		digest() // never blocks a session
	case "record-claudemd":
		// A non-zero exit that is not 2 surfaces the diagnostic without blocking
		// the tool call the hook fired for.
		if !recordClaudeMD() {
			os.Exit(1)
		}
	case "-h", "--help", "help":
		fmt.Fprint(os.Stderr, usage)
	default:
		fmt.Fprintf(os.Stderr, "unknown mode %q\n\n%s", mode, usage)
		os.Exit(2)
	}
}

const usage = `immuledger — tamper-evident decision ledger (embedded immudb)

Usage:
  immuledger serve            Run the MCP server over stdio
  immuledger digest           Print active-decisions digest (SessionStart hook)
  immuledger record-claudemd  Record a CLAUDE.md/.claude change (PostToolUse hook)

Environment:
  IMMULEDGER_DATA_DIR   Ledger data directory (default ~/.immuledger)
  PROJECT_DIR           Project root used to derive the project name
`

func newLedger() *ledger.Ledger {
	return ledger.New(ledger.DefaultDataDir())
}

func serve() error {
	return mcpserver.ServeStdio(mcp.NewServer(newLedger()))
}

// digest prints a compact list of the project's active decisions to stdout so
// the SessionStart hook injects it into context. Failures print nothing on
// stdout — whatever lands there becomes session context — but are reported on
// stderr rather than swallowed.
func digest() {
	defer reportPanic("digest")

	ctx := context.Background()
	l := newLedger()
	proj := project.Name(os.Getenv("CLAUDE_PROJECT_DIR"))

	const maxDecisions = 12
	rows, err := l.ListDecisions(ctx, proj, "active", "", maxDecisions)
	if err != nil {
		warnf("could not read decisions for project %q: %v", proj, err)
		return
	}
	stats, err := l.Stats(ctx, proj)
	if err != nil {
		warnf("could not read ledger stats for project %q: %v", proj, err)
		return
	}

	if stats.DecisionsTotal == 0 {
		fmt.Printf("[immuledger] No decisions recorded yet for project %q. "+
			"Use /decision to capture important choices.\n", proj)
		return
	}
	if len(rows) == 0 {
		return
	}

	var b strings.Builder
	fmt.Fprintf(&b, "[immuledger] Active decisions for project %q (%d active / %d total). "+
		"Honor these; use /check before non-trivial changes and /decision to record new ones.\n",
		proj, stats.DecisionsActive, stats.DecisionsTotal)
	for _, d := range rows {
		tags := ""
		if strings.TrimSpace(d.Tags) != "" {
			tags = "  [" + d.Tags + "]"
		}
		fmt.Fprintf(&b, "  #%d: %s%s\n", d.ID, d.Title, tags)
	}
	if stats.DecisionsActive > len(rows) {
		fmt.Fprintf(&b, "  ... and %d more (run /decisions to see all).\n", stats.DecisionsActive-len(rows))
	}
	fmt.Print(b.String())
}

// hookPayload is the subset of the PostToolUse hook JSON we need.
type hookPayload struct {
	ToolName  string `json:"tool_name"`
	Cwd       string `json:"cwd"`
	ToolInput struct {
		FilePath string `json:"file_path"`
		Path     string `json:"path"`
	} `json:"tool_input"`
}

// recordClaudeMD reads the PostToolUse hook payload on stdin and, when the edited
// file is CLAUDE.md or lives under .claude/, records a tamper-evident
// claude_md_change event with a sha256 of the new file contents.
const (
	maxHookStdin = 1 << 20 // 1 MiB cap on hook payloads
	maxHashBytes = 8 << 20 // hash at most 8 MiB of a changed file
)

// recordClaudeMD returns false when the change should have been recorded but
// was not, so the caller can exit non-zero.
func recordClaudeMD() (ok bool) {
	defer reportPanic("record-claudemd")

	raw, err := io.ReadAll(io.LimitReader(os.Stdin, maxHookStdin))
	if err != nil || len(strings.TrimSpace(string(raw))) == 0 {
		return true
	}
	var p hookPayload
	if json.Unmarshal(raw, &p) != nil {
		return true
	}
	filePath := p.ToolInput.FilePath
	if filePath == "" {
		filePath = p.ToolInput.Path
	}
	if filePath == "" {
		return true
	}

	// Resolve relative paths against the hook's cwd, not this process's cwd, so
	// tracking and hashing act on the file the tool actually touched.
	if !filepath.IsAbs(filePath) && p.Cwd != "" {
		filePath = filepath.Join(p.Cwd, filePath)
	}
	filePath = filepath.Clean(filePath)

	if !isTracked(filePath) {
		return true
	}

	digestHex := ""
	if f, oerr := os.Open(filePath); oerr == nil {
		h := sha256.New()
		if _, cerr := io.Copy(h, io.LimitReader(f, maxHashBytes)); cerr == nil {
			digestHex = fmt.Sprintf("%x", h.Sum(nil))
		}
		f.Close()
	}

	ctx := context.Background()
	l := newLedger()
	proj := project.Name(firstNonEmpty(p.Cwd, os.Getenv("CLAUDE_PROJECT_DIR")))

	if _, err := l.RecordEvent(ctx, proj, ledger.Event{
		Type:    "claude_md_change",
		Summary: fmt.Sprintf("%s changed via %s", filepath.Base(filePath), p.ToolName),
		Path:    filePath,
		Hash:    digestHex,
	}); err != nil {
		// Do not swallow this. Once the edit proceeds unrecorded, no later read
		// can tell "the file was not edited" from "the edit was not captured",
		// and /verify would present a gapped history as complete.
		reportGap(l.DataDir(), fmt.Sprintf(
			"failed to record claude_md_change for %s (project %q): %v", filePath, proj, err))
		return false
	}
	return true
}

// warnf reports on stderr. Hook stdout is either injected into session context
// (digest) or is the MCP JSON-RPC stream, so diagnostics never go there.
func warnf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "[immuledger] "+format+"\n", args...)
}

// reportGap records an audit-trail gap: on stderr for the user, and appended to
// a log beside the ledger so the gap is still discoverable afterwards.
func reportGap(dataDir, msg string) {
	warnf("%s", msg)
	if dataDir == "" {
		return
	}
	f, err := os.OpenFile(filepath.Join(dataDir, "hook-failures.log"),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return
	}
	defer f.Close()
	fmt.Fprintf(f, "%s %s\n", time.Now().UTC().Format(time.RFC3339), msg)
}

// reportPanic keeps a hook from taking down the session while still saying that
// something went wrong, instead of the blanket recover-and-forget it replaces.
func reportPanic(mode string) {
	if r := recover(); r != nil {
		warnf("%s hook panicked and was suppressed: %v", mode, r)
	}
}

func isTracked(filePath string) bool {
	base := filepath.Base(filePath)
	lower := strings.ToLower(strings.ReplaceAll(filePath, "\\", "/"))
	if strings.EqualFold(base, "CLAUDE.md") {
		return true
	}
	return strings.HasPrefix(lower, ".claude/") ||
		strings.Contains(lower, "/.claude/") ||
		strings.HasSuffix(lower, "/.claude")
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
