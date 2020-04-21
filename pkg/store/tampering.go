package store

import (
	"fmt"
	"os/exec"
	"strings"
	"time"
)

type Tampering struct {
	At      time.Time
	Pid     string
	Target  string
	User    string
	Command string
}

func (t *Tampering) withNames() {
	t.Pid = "PID"
	t.Target = "NAME"
	t.User = "USER"
	t.Command = "COMMAND"
}
func (t *Tampering) atIndexes(header string) map[string]int {
	indexes := map[string]int{}
	for i, field := range strings.Fields(header) {
		switch upperField := strings.ToUpper(field); upperField {
		case t.Pid, t.Target, t.User, t.Command:
			indexes[upperField] = i
		}
	}
	return indexes
}
func (t *Tampering) withValues(values []string, indexes map[string]int, ts time.Time, output *Tampering) {
	for field, j := range indexes {
		value := values[j]
		switch field {
		case t.Pid:
			output.Pid = value
		case t.Target:
			output.Target = value
		case t.User:
			output.User = value
		case t.Command:
			output.Command = value
		}
	}
	output.At = ts
}

func GetTamperings(dir string, pid string, file string) ([]Tampering, error) {
	lsofOutput, err := exec.Command("lsof", "+d", dir).Output()
	if err != nil {
		return nil, fmt.Errorf("lsof error: %v", err)
	}
	lsofRows := strings.Split(string(lsofOutput), "\n")
	if len(lsofRows) < 2 {
		return []Tampering{}, nil
	}
	wanted := Tampering{}
	wanted.withNames()
	atIndexes := wanted.atIndexes(lsofRows[0])

	now := time.Now()
	tamperings := []Tampering{}
	for i := 1; i < len(lsofRows); i++ {
		values := strings.Fields(lsofRows[i])
		pidIndex := atIndexes[wanted.Pid]
		if len(values) <= pidIndex || values[pidIndex] == pid {
			continue
		}
		tampering := Tampering{}
		wanted.withValues(values, atIndexes, now, &tampering)
		if tampering.Target == file {
			tamperings = append(tamperings, tampering)
		}
	}

	return tamperings, nil
}
