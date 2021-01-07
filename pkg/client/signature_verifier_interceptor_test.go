package client

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/signer"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"testing"
)

func TestSignatureVerifierInterceptor(t *testing.T) {

	pk, err := signer.ParsePublicKeyFile("./../../test/signer/ec1.pub")
	require.NoError(t, err)
	c := DefaultClient().WithPublicKey(pk)

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
	c := DefaultClient().WithPublicKey(pk)

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
	c := DefaultClient().WithPublicKey(pk)

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
	c := DefaultClient().WithPublicKey(nil)
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
