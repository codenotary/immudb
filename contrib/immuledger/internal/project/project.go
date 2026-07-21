// Package project derives a stable, human-friendly identifier for the repo the
// user is working in, so one immuledger data directory can serve many projects.
package project

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

// Name returns a stable project identifier: the git repository's top-level
// folder name, or the basename of startDir when it is not inside a repo.
//
// startDir may be empty, in which case PROJECT_DIR, then CLAUDE_PROJECT_DIR,
// then the current working directory are tried in order.
func Name(startDir string) string {
	dir := firstNonEmpty(
		startDir,
		os.Getenv("PROJECT_DIR"),
		os.Getenv("CLAUDE_PROJECT_DIR"),
	)
	if dir == "" {
		if wd, err := os.Getwd(); err == nil {
			dir = wd
		}
	}

	if top := gitToplevel(dir); top != "" {
		return sanitize(filepath.Base(top))
	}
	if dir != "" {
		if abs, err := filepath.Abs(dir); err == nil {
			return sanitize(filepath.Base(abs))
		}
	}
	return "default"
}

func gitToplevel(dir string) string {
	if dir == "" {
		return ""
	}
	cmd := exec.Command("git", "-C", dir, "rev-parse", "--show-toplevel")
	out, err := cmd.Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}

// sanitize keeps the project name usable as a key segment: it must not contain
// the '/' separator used to build ledger keys.
func sanitize(name string) string {
	name = strings.ReplaceAll(name, "/", "_")
	name = strings.TrimSpace(name)
	if name == "" {
		return "default"
	}
	return name
}
