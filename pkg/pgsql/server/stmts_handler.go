/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

var set = regexp.MustCompile(`(?i)set\s+.+`)
var selectVersion = regexp.MustCompile(`(?i)select\s+version\(\s*\)`)
var dealloc = regexp.MustCompile(`(?i)deallocate\s+\"([^\"]+)\"`)

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
	default:
		return pserr.ErrMessageCannotBeHandledInternally
	}
	return nil
}

type version struct{}

type deallocate struct {
	plan string
}
