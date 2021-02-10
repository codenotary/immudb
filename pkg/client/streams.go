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
	"bytes"
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"io"
)

func (c *immuClient) SetStream(ctx context.Context) (schema.ImmuService_SetStreamClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.SetStream(ctx)
}

func (c *immuClient) GetStream(ctx context.Context, in *schema.KeyRequest) (schema.ImmuService_GetStreamClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.GetStream(ctx, in)
}

func (c *immuClient) SetStr(ctx context.Context, kv *stream.KeyValue) (*schema.TxMetadata, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	s, err := c.SetStream(ctx)
	if err != nil {
		return nil, err
	}

	kvs := stream.NewKvStreamSender(stream.NewMsgSender(s))

	err = kvs.Send(kv)
	if err != nil {
		return nil, err
	}
	return s.CloseAndRecv()
}

func (c *immuClient) GetStr(ctx context.Context, k *schema.KeyRequest) (*schema.Entry, error) {
	gs, err := c.GetStream(ctx, k)

	kvr := stream.NewKvStreamReceiver(stream.NewMsgReceiver(gs))

	key, err := kvr.NextKey()
	if err != nil {
		return nil, err
	}

	vr, err := kvr.NextValueReader()
	if err != nil {
		return nil, err
	}

	b := bytes.NewBuffer([]byte{})
	vl := 0
	chunk := make([]byte, stream.ChunkSize)
	for {
		l, err := vr.Read(chunk)
		if err != nil && err != io.EOF {
			return nil, err
		}
		vl += l
		b.Write(chunk)
		if err == io.EOF || l == 0 {
			break
		}
	}
	value := make([]byte, vl)
	_, err = b.Read(value)
	if err != nil {
		return nil, err
	}

	return &schema.Entry{
		Key:   key,
		Value: value,
	}, nil
}
