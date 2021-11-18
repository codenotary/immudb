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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/metadata"
)

type TxOptions struct {
	ReadWrite bool
}

type Tx interface {
	Set(ctx context.Context, key []byte, value []byte) error
	Get(ctx context.Context, key []byte) (*schema.KeyValue, error)
	Scan(ctx context.Context, req *schema.TxScannerRequest) (*schema.TxScanneReponse, error)
	Commit(ctx context.Context) (*schema.TxHeader, error)
	Rollback(ctx context.Context) error
}

type tx struct {
	ic            *immuClient
	transactionID string
}

func (c *tx) Get(ctx context.Context, key []byte) (*schema.KeyValue, error) {
	ctx = c.populateContext(ctx)
	return c.ic.ServiceClient.TxGet(ctx, &schema.TxKeyRequest{Key: key})
}

func (c *tx) Set(ctx context.Context, key []byte, value []byte) error {
	ctx = c.populateContext(ctx)
	_, err := c.ic.ServiceClient.TxSet(ctx, &schema.TxSetRequest{KVs: []*schema.KeyValue{{
		Key:   key,
		Value: value,
	}}})
	return err
}

func (c *tx) Scan(ctx context.Context, request *schema.TxScannerRequest) (*schema.TxScanneReponse, error) {
	ctx = c.populateContext(ctx)
	return c.ic.ServiceClient.TxScanner(ctx, request)
}

func (c *tx) Commit(ctx context.Context) (*schema.TxHeader, error) {
	ctx = c.populateContext(ctx)
	_, err := c.ic.ServiceClient.Commit(ctx, new(empty.Empty))
	return nil, err
}

func (c *tx) Rollback(ctx context.Context) error {
	ctx = c.populateContext(ctx)
	_, err := c.ic.ServiceClient.Rollback(ctx, new(empty.Empty))
	return err
}

func (c *immuClient) BeginTx(ctx context.Context, options *TxOptions) (Tx, error) {
	r, err := c.ServiceClient.BeginTx(ctx, &schema.BeginTxRequest{
		ReadWrite: options.ReadWrite,
	})
	if err != nil {
		return nil, err
	}
	tx := &tx{
		ic:            c,
		transactionID: r.TransactionID,
	}
	return tx, nil
}

func (c *tx) populateContext(ctx context.Context) context.Context {
	if c.transactionID != "" {
		ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("txid", c.transactionID))
	}
	return ctx
}
