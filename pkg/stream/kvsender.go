/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

// NewKvStreamSender returns a new kvStreamSender
func NewKvStreamSender(s MsgSender) *kvStreamSender {
	return &kvStreamSender{
		s: s,
	}
}

// Send send a KeyValue on strem
func (st *kvStreamSender) Send(kv *KeyValue) error {
	vss := []*ValueSize{kv.Key, kv.Value}
	for _, vs := range vss {
		err := st.send(vs)
		if err != nil {
			return err
		}
	}
	return nil
}

func (st *kvStreamSender) send(vs *ValueSize) error {
	err := st.s.Send(vs.Content, vs.Size)
	if err != nil {
		if err == io.EOF {
			return st.s.RecvMsg(nil)
		}
		return err
	}
	return nil
}
