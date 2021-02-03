/*
Copyright 2019-2020 vChain, Inc.

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

package client

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
)

func (c *immuClient) Stream(ctx context.Context) (schema.ImmuService_StreamClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.Stream(ctx)
}

type KvStreamSender interface {
	Send(kv *stream.KeyValue) error
}

type kvStreamSender struct {
	s stream.MsgSender
}

func NewKvStreamSender(s stream.MsgSender) *kvStreamSender {
	return &kvStreamSender{
		s: s,
	}
}

func (st *kvStreamSender) Send(kv *stream.KeyValue) error {
	err := st.s.Send(kv.Key.Content, kv.Key.Size)
	if err != nil {
		return err
	}
	c, err := st.s.Recv()
	if err != nil {
		return err
	}
	err = st.s.Send(kv.Value.Content, kv.Value.Size)
	if err != nil {
		return err
	}

	c, err = st.s.Recv()
	if err != nil {
		return err
	}

	println(string(c))

	return nil
}
