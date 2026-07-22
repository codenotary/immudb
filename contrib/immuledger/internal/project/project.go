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
	if strings.Contains(startDir, "${") { // ignore an unexpanded placeholder
		startDir = ""
	}
	dir := firstNonEmpty(
		strings.TrimSpace(startDir),
		cleanEnv("PROJECT_DIR"),
		cleanEnv("CLAUDE_PROJECT_DIR"),
	)
	if dir == "" {
		if wd, err := os.Getwd(); err == nil {
			dir = wd
		}
	}

	if top := gitToplevel(dir); top != "" {
		return Sanitize(filepath.Base(top))
	}
	if dir != "" {
		if abs, err := filepath.Abs(dir); err == nil {
			return Sanitize(filepath.Base(abs))
		}
	}
	return "default"
}

// cleanEnv returns a trimmed env value, treating an unexpanded "${VAR}"
// placeholder (which the plugin runtime passes when the variable is unset) as
// empty so it is never used as a literal path.
func cleanEnv(name string) string {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" || strings.Contains(v, "${") {
		return ""
	}
	return v
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

// maxProjectLen bounds the project segment so a caller-supplied override cannot
// produce unbounded keys.
const maxProjectLen = 128

// Sanitize keeps a project name usable and safe as a key segment: it strips the
// '/' separator used to build ledger keys, removes control characters, trims,
// and length-limits. Applied to both auto-detected names and caller overrides.
func Sanitize(name string) string {
	name = strings.Map(func(r rune) rune {
		if r == '/' || r == '\\' || r < 0x20 {
			return '_'
		}
		return r
	}, name)
	name = strings.TrimSpace(name)
	if name == "" {
		return "default"
	}
	if len(name) > maxProjectLen {
		name = name[:maxProjectLen]
	}
	return name
}
