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

package fmessages

import (
	"bufio"
	"bytes"
)

// Once a portal exists, it can be executed using an Execute message. The Execute message specifies the portal name
// (empty string denotes the unnamed portal) and a maximum result-row count (zero meaning “fetch all rows”).
// The result-row count is only meaningful for portals containing commands that return row sets; in other cases the
// command is always executed to completion, and the row count is ignored. The possible responses to Execute are the
// same as those described above for queries issued via simple query protocol, except that Execute doesn't cause
// ReadyForQuery or RowDescription to be issued.
//
// If Execute terminates before completing the execution of a portal (due to reaching a nonzero result-row count), it
// will send a PortalSuspended message; the appearance of this message tells the frontend that another Execute should be
// issued against the same portal to complete the operation. The CommandComplete message indicating completion of the
// source SQL command is not sent until the portal's execution is completed. Therefore, an Execute phase is always
// terminated by the appearance of exactly one of these messages: CommandComplete, EmptyQueryResponse (if the portal was
// created from an empty query string), ErrorResponse, or PortalSuspended.
type Execute struct {
	// The name of the portal to execute (an empty string selects the unnamed portal).
	PortalName string
	// Maximum number of rows to return, if portal contains a query that returns rows (ignored otherwise). Zero denotes “no limit”.
	MaxRows int32
}

func ParseExecuteMsg(payload []byte) (Execute, error) {
	b := bytes.NewBuffer(payload)
	r := bufio.NewReaderSize(b, len(payload))
	portalName, err := getNextString(r)
	if err != nil {
		return Execute{}, err
	}
	maxRows, err := getNextInt32(r)
	if err != nil {
		return Execute{}, err
	}
	return Execute{
		PortalName: portalName,
		MaxRows:    maxRows,
	}, nil
}
