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

package streamtest

import (
	"io"
)

type msgSenderMock struct {
	SendF    func(reader io.Reader, payloadSize int) (err error)
	RecvMsgF func(m interface{}) error
}

func DefaultMsgSenderMock(s *ImmuServiceSender_StreamMock, chunkSize int) *msgSenderMock {
	return &msgSenderMock{
		SendF: func(reader io.Reader, payloadSize int) (err error) {
			return nil
		},
		RecvMsgF: func(m interface{}) error {
			return nil
		},
	}
}

func (st *msgSenderMock) Send(reader io.Reader, payloadSize int) (err error) {
	return st.SendF(reader, payloadSize)
}

func (st *msgSenderMock) RecvMsg(m interface{}) error {
	return st.RecvMsgF(m)
}
