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

type DescribeMsg struct {
	// 'S' to describe a prepared statement; or 'P' to describe a portal.
	DescType string
	// The name of the prepared statement or portal to describe (an empty string selects the unnamed prepared statement or portal).
	Name string
}

func ParseDescribeMsg(msg []byte) (DescribeMsg, error) {
	descType := msg[0]
	return DescribeMsg{
		DescType: string(descType),
		Name:     string(msg[1 : len(msg)-1]),
	}, nil
}
