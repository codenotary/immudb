/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package server

import (
	"regexp"

	pserr "github.com/codenotary/immudb/pkg/pgsql/errors"
)

var (
	set           = regexp.MustCompile(`(?i)set\s+.+`)
	selectVersion = regexp.MustCompile(`(?i)select\s+version\(\s*\)`)
	dealloc       = regexp.MustCompile(`(?i)deallocate\s+\"([^\"]+)\"`)

	// immudb verification functions exposed via PG wire protocol
	immudbStateRe     = regexp.MustCompile(`(?i)select\s+immudb_state\(\s*\)`)
	immudbVerifyRowRe = regexp.MustCompile(`(?i)select\s+immudb_verify_row\(\s*(.+)\s*\)`)
	immudbVerifyTxRe  = regexp.MustCompile(`(?i)select\s+immudb_verify_tx\(\s*(.+)\s*\)`)
	immudbHistoryRe   = regexp.MustCompile(`(?i)select\s+immudb_history\(\s*(.+)\s*\)`)
	immudbTxRe        = regexp.MustCompile(`(?i)select\s+immudb_tx\(\s*(.+)\s*\)`)

	// SHOW statement patterns for ORM compatibility
	showRe = regexp.MustCompile(`(?i)^\s*show\s+(\w+)\s*;?\s*$`)
)

func (s *session) isInBlackList(statement string) bool {
	if set.MatchString(statement) {
		return true
	}

	if statement == ";" {
		return true
	}
	return false
}

func (s *session) isEmulableInternally(statement string) interface{} {
	if selectVersion.MatchString(statement) {
		return &version{}
	}

	if dealloc.MatchString(statement) {
		matches := dealloc.FindStringSubmatch(statement)
		if len(matches) == 2 {
			return &deallocate{plan: matches[1]}
		}
	}

	if immudbStateRe.MatchString(statement) {
		return &immudbStateCmd{}
	}

	if m := immudbVerifyRowRe.FindStringSubmatch(statement); len(m) == 2 {
		return &immudbVerifyRowCmd{args: m[1]}
	}

	if m := immudbVerifyTxRe.FindStringSubmatch(statement); len(m) == 2 {
		return &immudbVerifyTxCmd{args: m[1]}
	}

	if m := immudbHistoryRe.FindStringSubmatch(statement); len(m) == 2 {
		return &immudbHistoryCmd{args: m[1]}
	}

	if m := immudbTxRe.FindStringSubmatch(statement); len(m) == 2 {
		return &immudbTxCmd{args: m[1]}
	}

	if m := showRe.FindStringSubmatch(statement); len(m) == 2 {
		return &showCmd{param: m[1]}
	}

	return nil
}

func (s *session) tryToHandleInternally(command interface{}) error {
	switch cmd := command.(type) {
	case *version:
		if err := s.writeVersionInfo(); err != nil {
			return err
		}
	case *deallocate:
		delete(s.statements, cmd.plan)
		return nil
	case *immudbStateCmd:
		return s.immudbState()
	case *immudbVerifyRowCmd:
		return s.immudbVerifyRow(cmd.args)
	case *immudbVerifyTxCmd:
		return s.immudbVerifyTx(cmd.args)
	case *immudbHistoryCmd:
		return s.immudbHistory(cmd.args)
	case *immudbTxCmd:
		return s.immudbTxByID(cmd.args)
	case *showCmd:
		return s.handleShow(cmd.param)
	default:
		return pserr.ErrMessageCannotBeHandledInternally
	}
	return nil
}

type version struct{}

type deallocate struct {
	plan string
}

type immudbStateCmd struct{}

type immudbVerifyRowCmd struct {
	args string
}

type immudbVerifyTxCmd struct {
	args string
}

type immudbHistoryCmd struct {
	args string
}

type immudbTxCmd struct {
	args string
}

type showCmd struct {
	param string
}
