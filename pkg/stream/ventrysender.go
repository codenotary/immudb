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

type vEntryStreamSender struct {
	s MsgSender
}

func NewVEntryStreamSender(s MsgSender) *vEntryStreamSender {
	return &vEntryStreamSender{
		s: s,
	}
}

func (vess *vEntryStreamSender) Send(ve *VerifiableEntry) error {
	ves := []*ValueSize{ve.EntryWithoutValueProto, ve.VerifiableTxProto, ve.InclusionProofProto, ve.Value}
	for _, vs := range ves {
		err := vess.s.Send(vs.Content, vs.Size, nil)
		if errors.Is(err, io.EOF) {
			return vess.s.RecvMsg(nil)
		}
		if err != nil {
			return err
		}
	}
	return nil
}
