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

package errors

import (
	"context"
	stdError "errors"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"testing"
)

func TestClientUnaryInterceptor(t *testing.T) {

	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return &struct{}{}, nil
	}
	i, err := ServerUnaryInterceptor(context.Background(), "method", nil, h)
	require.Nil(t, err)
	require.NotNil(t, i)
}

func TestClientUnaryInterceptorError(t *testing.T) {

	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, stdError.New("stdError")
	}
	_, err := ServerUnaryInterceptor(context.Background(), "method", nil, h)
	require.Error(t, err)
	require.Implements(t, (*errors.ImmuError)(nil), err)
}

func TestClientUnaryInterceptorImmuError(t *testing.T) {

	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, errors.New("stdError")
	}
	_, err := ServerUnaryInterceptor(context.Background(), "method", nil, h)
	require.Error(t, err)
	require.Implements(t, (*errors.ImmuError)(nil), err)
}

func TestServerStreamInterceptor(t *testing.T) {
	h := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}

	err := ServerStreamInterceptor(nil, &MockedServerStream{}, nil, h)

	require.Nil(t, err)
}

func TestServerStreamInterceptorError(t *testing.T) {
	h := func(srv interface{}, stream grpc.ServerStream) error {
		return stdError.New("stdError")
	}

	err := ServerStreamInterceptor(nil, &MockedServerStream{}, nil, h)

	require.Error(t, err)
	require.Implements(t, (*errors.ImmuError)(nil), err)
}

func TestServerStreamInterceptorImmuError(t *testing.T) {
	h := func(srv interface{}, stream grpc.ServerStream) error {
		return errors.New("immuError")
	}

	err := ServerStreamInterceptor(nil, &MockedServerStream{}, nil, h)

	require.Error(t, err)
	require.Implements(t, (*errors.ImmuError)(nil), err)
}

type MockedServerStream struct{}

func (ss *MockedServerStream) SetHeader(metadata.MD) error {
	return nil
}

func (ss *MockedServerStream) SendHeader(metadata.MD) error {
	return nil
}

func (ss *MockedServerStream) SetTrailer(metadata.MD) {}

func (ss *MockedServerStream) Context() context.Context {
	return context.TODO()
}

func (ss *MockedServerStream) SendMsg(m interface{}) error {
	return nil
}

func (ss *MockedServerStream) RecvMsg(m interface{}) error {
	return nil
}
