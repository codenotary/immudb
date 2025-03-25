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

package streamtest

type MsgError struct {
	M []byte
	E error
}

type msgReceiverMock struct {
	c     int
	me    []*MsgError
	ReadF func(message []byte) (n int, err error)
}

func DefaultMsgReceiverMock(me []*MsgError) *msgReceiverMock {
	r := &msgReceiverMock{me: me}
	f := func(message []byte) (n int, err error) {
		l := copy(message, r.me[r.c].M)
		e := r.me[r.c].E
		r.c++
		return l, e
	}
	r.ReadF = f
	return r
}

func (st *msgReceiverMock) Read(message []byte) (n int, err error) {
	return st.ReadF(message)
}
