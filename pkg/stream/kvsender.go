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

type kvStreamSender struct {
	s MsgSender
}

func NewKvStreamSender(s MsgSender) *kvStreamSender {
	return &kvStreamSender{
		s: s,
	}
}

func (st *kvStreamSender) Send(kv *KeyValue) error {
	err := st.s.Send(kv.Key.Content, kv.Key.Size)
	if err != nil {
		if err == io.EOF {
			return st.s.RecvMsg(nil)
		}
		return err
	}

	err = st.s.Send(kv.Value.Content, kv.Value.Size)
	if err != nil {
		if err == io.EOF {
			return st.s.RecvMsg(nil)
		}
		return err
	}

	return nil
}
