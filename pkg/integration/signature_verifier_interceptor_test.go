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
package integration

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	ic "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestSignatureVerifierInterceptor(t *testing.T) {

	pk, err := signer.ParsePublicKeyFile("./../../test/signer/ec1.pub")
	require.NoError(t, err)
	c := ic.DefaultClient().WithServerSigningPubKey(pk)

	// creation and state sign
	state := &schema.ImmutableState{
		TxId:   0,
		TxHash: []byte(`hash`),
	}
	sig, err := signer.NewSigner("./../../test/signer/ec1.key")
	require.NoError(t, err)
	stSig := server.NewStateSigner(sig)
	err = stSig.Sign(state)
	require.NoError(t, err)

	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	err = c.SignatureVerifierInterceptor(context.TODO(), "/immudb.schema.ImmuService/CurrentState", &empty.Empty{}, state, nil, invoker, nil)

	require.NoError(t, err)

}

func TestSignatureVerifierInterceptorUnableToVerify(t *testing.T) {
	pk, err := signer.ParsePublicKeyFile("./../../test/signer/ec1.pub")
	require.NoError(t, err)
	c := ic.DefaultClient().WithServerSigningPubKey(pk)

	// creation and state sign
	state := &schema.ImmutableState{
		TxId:   0,
		TxHash: []byte(`hash`),
		Signature: &schema.Signature{
			PublicKey: []byte(`test`),
			Signature: []byte(`boom`),
		},
	}
	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}
	err = c.SignatureVerifierInterceptor(context.TODO(), "/immudb.schema.ImmuService/CurrentState", &empty.Empty{}, state, nil, invoker, nil)
	require.Error(t, err)
}

func TestSignatureVerifierInterceptorSignatureDoesntMatch(t *testing.T) {
	pk, err := signer.ParsePublicKeyFile("./../../test/signer/ec1.pub")
	require.NoError(t, err)
	c := ic.DefaultClient().WithServerSigningPubKey(pk)

	// creation and state sign
	state := &schema.ImmutableState{
		TxId:   0,
		TxHash: []byte(`hash`),
	}
	sig, err := signer.NewSigner("./../../test/signer/ec3.key")
	require.NoError(t, err)
	stSig := server.NewStateSigner(sig)
	err = stSig.Sign(state)
	require.NoError(t, err)

	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	err = c.SignatureVerifierInterceptor(context.TODO(), "/immudb.schema.ImmuService/CurrentState", &empty.Empty{}, state, nil, invoker, nil)

	require.Error(t, err)
}

func TestSignatureVerifierInterceptorNoPublicKey(t *testing.T) {
	c := ic.DefaultClient().WithServerSigningPubKey(nil)
	// creation and state sign
	state := &schema.ImmutableState{
		TxId:   0,
		TxHash: []byte(`hash`),
	}

	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}

	err := c.SignatureVerifierInterceptor(context.TODO(), "/immudb.schema.ImmuService/CurrentState", &empty.Empty{}, state, nil, invoker, nil)

	require.Error(t, err)
}
