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

// CopyDataMsg carries a chunk of COPY data from the client (message type 'd').
type CopyDataMsg struct {
	Data []byte
}

// ParseCopyDataMsg parses a CopyData frontend message.
func ParseCopyDataMsg(payload []byte) (CopyDataMsg, error) {
	return CopyDataMsg{Data: payload}, nil
}

// CopyDoneMsg signals the end of COPY data from the client (message type 'c').
type CopyDoneMsg struct{}

// ParseCopyDoneMsg parses a CopyDone frontend message.
func ParseCopyDoneMsg(payload []byte) (CopyDoneMsg, error) {
	return CopyDoneMsg{}, nil
}

// CopyFailMsg signals a client-side COPY failure (message type 'f').
type CopyFailMsg struct {
	Error string
}

// ParseCopyFailMsg parses a CopyFail frontend message.
func ParseCopyFailMsg(payload []byte) (CopyFailMsg, error) {
	// Error message is a null-terminated string
	msg := string(payload)
	if len(msg) > 0 && msg[len(msg)-1] == 0 {
		msg = msg[:len(msg)-1]
	}
	return CopyFailMsg{Error: msg}, nil
}
