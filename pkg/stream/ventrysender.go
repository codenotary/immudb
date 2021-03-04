/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stream

import "io"

type vEntryStreamSender struct {
	s MsgSender
}

func NewVEntryStreamSender(s MsgSender) *vEntryStreamSender {
	return &vEntryStreamSender{
		s: s,
	}
}

func (vess *vEntryStreamSender) Send(ve *VerifiableEntry) error {
	err := vess.s.Send(ve.EntryWithoutValueProto.Content, ve.EntryWithoutValueProto.Size)
	if err != nil {
		if err == io.EOF {
			return vess.s.RecvMsg(nil)
		}
		return err
	}

	err = vess.s.Send(ve.VerifiableTxProto.Content, ve.VerifiableTxProto.Size)
	if err != nil {
		if err == io.EOF {
			return vess.s.RecvMsg(nil)
		}
		return err
	}

	err = vess.s.Send(ve.InclusionProofProto.Content, ve.InclusionProofProto.Size)
	if err != nil {
		if err == io.EOF {
			return vess.s.RecvMsg(nil)
		}
		return err
	}

	err = vess.s.Send(ve.Value.Content, ve.Value.Size)
	if err != nil {
		if err == io.EOF {
			return vess.s.RecvMsg(nil)
		}
		return err
	}

	return nil
}
