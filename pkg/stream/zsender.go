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

package stream

import (
	"errors"
	"io"
)

type zStreamSender struct {
	s MsgSender
}

// NewZStreamSender ...
func NewZStreamSender(s MsgSender) *zStreamSender {
	return &zStreamSender{
		s: s,
	}
}

func (st *zStreamSender) Send(ze *ZEntry) error {
	for _, vs := range []*ValueSize{ze.Set, ze.Key, ze.Score, ze.AtTx, ze.Value} {
		err := st.s.Send(vs.Content, vs.Size, nil)
		if errors.Is(err, io.EOF) {
			return st.s.RecvMsg(nil)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
