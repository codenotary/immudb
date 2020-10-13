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

package auth

import (
	"context"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
)

func TestTokenAuth(t *testing.T) {
	token := TokenAuth{
		Token: "123456",
	}
	m, err := token.GetRequestMetadata(context.Background(), "")
	if err != nil {
		t.Errorf("Error GetRequestMetadata %s", err)
	}
	header, ok := m["authorization"]
	if !ok {
		t.Errorf("Error GetRequestMetadata expected authorization header is missing")
	}
	if header != "Bearer "+token.Token {
		t.Errorf("Error GetRequestMetadata wrong authorization header")
	}
	if token.RequireTransportSecurity() {
		t.Errorf("Error RequireTransportSecurity expected to return false")
	}
}

func TestClientUnaryInterceptor(t *testing.T) {
	f := ClientUnaryInterceptor("token")
	invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
		return nil
	}
	err := f(context.Background(), "", "", "", nil, invoker)
	assert.Nil(t, err)
}

func TestClientStreamInterceptor(t *testing.T) {
	f := ClientStreamInterceptor("token")
	streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		return nil, nil
	}
	_, err := f(context.Background(), nil, nil, "", streamer)
	assert.Nil(t, err)
}
