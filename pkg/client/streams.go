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

package client

import (
	"context"
	"io"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
)

func (c *immuClient) streamSet(ctx context.Context) (schema.ImmuService_StreamSetClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamSet(ctx)
}

func (c *immuClient) streamGet(ctx context.Context, in *schema.KeyRequest) (schema.ImmuService_StreamGetClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamGet(ctx, in)
}

func (c *immuClient) streamScan(ctx context.Context, in *schema.ScanRequest) (schema.ImmuService_StreamScanClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamScan(ctx, in)
}

func (c *immuClient) streamZScan(ctx context.Context, in *schema.ZScanRequest) (schema.ImmuService_StreamZScanClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamZScan(ctx, in)
}

func (c *immuClient) streamHistory(ctx context.Context, in *schema.HistoryRequest) (schema.ImmuService_StreamHistoryClient, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	return c.ServiceClient.StreamHistory(ctx, in)
}

func (c *immuClient) StreamSet(ctx context.Context, kvs []*stream.KeyValue) (*schema.TxMetadata, error) {
	if !c.IsConnected() {
		return nil, ErrNotConnected
	}
	s, err := c.streamSet(ctx)
	if err != nil {
		return nil, err
	}

	kvss := stream.NewKvStreamSender(stream.NewMsgSender(s, c.Options.StreamChunkSize))

	for _, kv := range kvs {
		err = kvss.Send(kv)
		if err != nil {
			return nil, err
		}
	}

	return s.CloseAndRecv()
}

func (c *immuClient) StreamGet(ctx context.Context, k *schema.KeyRequest) (*schema.Entry, error) {
	gs, err := c.streamGet(ctx, k)

	kvr := stream.NewKvStreamReceiver(stream.NewMsgReceiver(gs), c.Options.StreamChunkSize)

	key, vr, err := kvr.Next()
	if err != nil {
		return nil, err
	}

	return stream.ParseKV(key, vr, c.Options.StreamChunkSize)
}

func (c *immuClient) StreamScan(ctx context.Context, req *schema.ScanRequest) (*schema.Entries, error) {
	gs, err := c.streamScan(ctx, req)
	if err != nil {
		return nil, err
	}
	kvr := c.Ssf.NewKvStreamReceiver(gs)
	var entries []*schema.Entry
	for {
		key, vr, err := kvr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entry, err := stream.ParseKV(key, vr, c.Options.StreamChunkSize)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return &schema.Entries{Entries: entries}, nil
}

func (c *immuClient) StreamZScan(ctx context.Context, req *schema.ZScanRequest) (*schema.ZEntries, error) {
	gs, err := c.streamZScan(ctx, req)
	if err != nil {
		return nil, err
	}
	zr := c.Ssf.NewZStreamReceiver(gs)
	var entries []*schema.ZEntry
	for {
		set, key, score, vr, err := zr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entry, err := stream.ParseZEntry(set, key, score, vr, c.Options.StreamChunkSize)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return &schema.ZEntries{Entries: entries}, nil
}

func (c *immuClient) StreamHistory(ctx context.Context, req *schema.HistoryRequest) (*schema.Entries, error) {
	gs, err := c.streamHistory(ctx, req)
	if err != nil {
		return nil, err
	}
	kvr := c.Ssf.NewKvStreamReceiver(gs)
	var entries []*schema.Entry
	for {
		key, vr, err := kvr.Next()
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		entry, err := stream.ParseKV(key, vr, c.Options.StreamChunkSize)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	return &schema.Entries{Entries: entries}, nil
}
