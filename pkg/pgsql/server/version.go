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
	"github.com/codenotary/immudb/embedded/sql"
	bm "github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	"github.com/codenotary/immudb/pkg/pgsql/server/pgmeta"
)

func (s *session) writeVersionInfo() error {
	cols := []sql.ColDescriptor{{Column: "version", Type: sql.VarcharType}}
	if _, err := s.writeMessage(bm.RowDescription(cols, nil)); err != nil {
		return err
	}

	value := sql.NewVarchar(pgmeta.PgsqlServerVersionMessage)
	rows := []*sql.Row{{
		ValuesByPosition: []sql.TypedValue{value},
		ValuesBySelector: map[string]sql.TypedValue{"version": value},
	}}
	if _, err := s.writeMessage(bm.DataRow(rows, len(cols), nil)); err != nil {
		return err
	}

	return nil
}
