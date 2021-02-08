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

package auth

import (
	"context"
	"net"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type MockedServerStream struct {
}

func (ss *MockedServerStream) SetHeader(metadata.MD) error {
	return nil
}

func (ss *MockedServerStream) SendHeader(metadata.MD) error {
	return nil
}

func (ss *MockedServerStream) SetTrailer(metadata.MD) {

}

func (ss *MockedServerStream) Context() context.Context {
	ip := net.IP{}
	ip.UnmarshalText([]byte(`10.0.0.1`))
	p := &peer.Peer{
		Addr: &net.TCPAddr{
			IP:   ip,
			Port: 9999,
			Zone: "zone",
		},
	}

	return peer.NewContext(context.TODO(), p)
}

func (ss *MockedServerStream) SendMsg(m interface{}) error {
	return nil
}

func (ss *MockedServerStream) RecvMsg(m interface{}) error {
	return nil
}

func TestServerStreamInterceptor(t *testing.T) {
	UpdateMetrics = func(context.Context) {
	}

	IsTampered = false
	AuthEnabled = true

	h := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}

	sh := ServerStreamInterceptor(nil, &MockedServerStream{}, nil, h)
	require.Nil(t, sh)

}

func TestServerStreamInterceptorTampered(t *testing.T) {
	UpdateMetrics = func(context.Context) {
	}

	IsTampered = true
	AuthEnabled = true

	h := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}

	sh := ServerStreamInterceptor(nil, &MockedServerStream{}, nil, h)
	require.Error(t, sh)

}

func TestServerStreamInterceptorNoAuth(t *testing.T) {
	UpdateMetrics = func(context.Context) {
	}

	IsTampered = false
	AuthEnabled = false

	h := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}

	sh := ServerStreamInterceptor(nil, &MockedServerStream{}, nil, h)
	require.Error(t, sh)

}

func TestServerUnaryInterceptor(t *testing.T) {
	UpdateMetrics = func(context.Context) {
	}
	IsTampered = false
	AuthEnabled = true

	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	}

	r, err := ServerUnaryInterceptor(context.Background(), "method", nil, h)
	require.NoError(t, err)
	require.Nil(t, r)
}

func TestServerUnaryInterceptorTampered(t *testing.T) {
	UpdateMetrics = func(context.Context) {
	}
	IsTampered = true
	AuthEnabled = true

	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	}

	_, err := ServerUnaryInterceptor(context.Background(), "method", nil, h)
	require.Error(t, err)
}

func TestServerUnaryInterceptorNoAuth(t *testing.T) {
	UpdateMetrics = func(context.Context) {
	}
	IsTampered = false
	AuthEnabled = false

	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	}

	_, err := ServerUnaryInterceptor(context.Background(), "method", nil, h)
	require.Error(t, err)
}
