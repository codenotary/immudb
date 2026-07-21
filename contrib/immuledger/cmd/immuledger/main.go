// Command immuledger is the single binary behind the immuledger Claude Code
// plugin. It has three modes:
//
//	immuledger serve            run the MCP server over stdio (the plugin's mcpServer)
//	immuledger digest           SessionStart hook: print a digest of active decisions
//	immuledger record-claudemd  PostToolUse hook: record CLAUDE.md/.claude changes
//
// All modes embed immudb in-process (no server, no container). The hook modes
// are deliberately fail-silent so they never block or disrupt a session.
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
		digest() // fail-silent
	case "record-claudemd":
		recordClaudeMD() // fail-silent
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
// the SessionStart hook injects it into context. Any failure prints nothing.
func digest() {
	defer func() { _ = recover() }()

	ctx := context.Background()
	l := newLedger()
	proj := project.Name(os.Getenv("CLAUDE_PROJECT_DIR"))

	const maxDecisions = 12
	rows, err := l.ListDecisions(ctx, proj, "active", "", maxDecisions)
	if err != nil {
		return
	}
	stats, err := l.Stats(ctx, proj)
	if err != nil {
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

func recordClaudeMD() {
	defer func() { _ = recover() }()

	raw, err := io.ReadAll(io.LimitReader(os.Stdin, maxHookStdin))
	if err != nil || len(strings.TrimSpace(string(raw))) == 0 {
		return
	}
	var p hookPayload
	if json.Unmarshal(raw, &p) != nil {
		return
	}
	filePath := p.ToolInput.FilePath
	if filePath == "" {
		filePath = p.ToolInput.Path
	}
	if filePath == "" {
		return
	}

	// Resolve relative paths against the hook's cwd, not this process's cwd, so
	// tracking and hashing act on the file the tool actually touched.
	if !filepath.IsAbs(filePath) && p.Cwd != "" {
		filePath = filepath.Join(p.Cwd, filePath)
	}
	filePath = filepath.Clean(filePath)

	if !isTracked(filePath) {
		return
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

	_, _ = l.RecordEvent(ctx, proj, ledger.Event{
		Type:    "claude_md_change",
		Summary: fmt.Sprintf("%s changed via %s", filepath.Base(filePath), p.ToolName),
		Path:    filePath,
		Hash:    digestHex,
	})
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
