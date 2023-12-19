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

package fmessages

import (
	"bufio"
	"bytes"
)

type ParseMsg struct {
	// The number of parameter data types specified (can be zero). Note that this is not an indication of the number of parameters that might appear in the query string, only the number that the frontend wants to prespecify types for.
	ParamsCount int16
	// The name of the destination prepared statement (an empty string selects the unnamed prepared statement).
	DestPreparedStatementName string
	// The query string to be parsed.
	Statements string
	// Specifies the object IDs of the parameters data type. Placing a zero here is equivalent to leaving the type unspecified.
	ObjectIDs []int32
}

func ParseParseMsg(payload []byte) (ParseMsg, error) {
	b := bytes.NewBuffer(payload)
	r := bufio.NewReaderSize(b, len(payload))
	destPreparedStatementName, err := getNextString(r)
	if err != nil {
		return ParseMsg{}, err
	}
	queryString, err := getNextString(r)
	if err != nil {
		return ParseMsg{}, err
	}

	pCount, err := getNextInt16(r)
	if err != nil {
		return ParseMsg{}, err
	}

	objectIDs := make([]int32, 0)

	for k := int16(0); k < pCount; k++ {
		ID, err := getNextInt32(r)
		if err != nil {
			return ParseMsg{}, err
		}
		objectIDs = append(objectIDs, ID)
	}

	return ParseMsg{DestPreparedStatementName: destPreparedStatementName, Statements: queryString, ParamsCount: pCount, ObjectIDs: objectIDs}, nil
}
