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
package gw

import (
	"context"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	immuclient "github.com/codenotary/immudb/pkg/client"
	"google.golang.org/grpc"
)

type immuClientMock struct {
	immuclient.ImmuClient

	safeGet       func(context.Context, []byte, ...grpc.CallOption) (*client.VerifiedItem, error)
	safeSet       func(context.Context, []byte, []byte) (*client.VerifiedIndex, error)
	set           func(context.Context, []byte, []byte) (*schema.Index, error)
	safeReference func(context.Context, []byte, []byte) (*client.VerifiedIndex, error)
	safeZAdd      func(context.Context, []byte, float64, []byte) (*client.VerifiedIndex, error)
	history       func(context.Context, []byte) (*schema.StructuredItemList, error)
}

func (icm *immuClientMock) SafeGet(ctx context.Context, key []byte, opts ...grpc.CallOption) (*client.VerifiedItem, error) {
	return icm.safeGet(ctx, key)
}
func (icm *immuClientMock) SafeSet(ctx context.Context, key []byte, value []byte) (*client.VerifiedIndex, error) {
	return icm.safeSet(ctx, key, value)
}
func (icm *immuClientMock) Set(ctx context.Context, key []byte, value []byte) (*schema.Index, error) {
	return icm.set(ctx, key, value)
}
func (icm *immuClientMock) SafeReference(ctx context.Context, reference []byte, key []byte) (*client.VerifiedIndex, error) {
	return icm.safeReference(ctx, reference, key)
}
func (icm *immuClientMock) SafeZAdd(ctx context.Context, set []byte, score float64, key []byte) (*client.VerifiedIndex, error) {
	return icm.safeZAdd(ctx, set, score, key)
}
func (icm *immuClientMock) History(ctx context.Context, key []byte) (*schema.StructuredItemList, error) {
	return icm.history(ctx, key)
}
