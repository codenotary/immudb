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

package server

import (
	"context"
	"github.com/codenotary/immudb/pkg/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"testing"
	"time"
)

func TestContextHandlerInterceptor(t *testing.T) {
	req := struct{}{}
	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		return nil, nil
	}
	_, err := ContextHandlerInterceptor(context.TODO(), req, nil, h)
	require.NoError(t, err)
}

func TestContextHandlerInterceptor_Canceled(t *testing.T) {
	req := struct{}{}
	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		time.Sleep(100 * time.Second)
		return nil, nil
	}
	ctx, cancel := context.WithCancel(context.TODO())
	cancel()
	_, err := ContextHandlerInterceptor(ctx, req, nil, h)
	require.ErrorIs(t, errors.New(ErrCanceled), err)
}

func TestContextHandlerInterceptor_DeadlineExceeded(t *testing.T) {
	req := struct{}{}
	h := func(ctx context.Context, req interface{}) (interface{}, error) {
		time.Sleep(100 * time.Second)
		return nil, nil
	}
	ctx, _ := context.WithTimeout(context.TODO(), time.Nanosecond*1)
	_, err := ContextHandlerInterceptor(ctx, req, nil, h)
	require.ErrorIs(t, errors.New(ErrDeadlineExceeded), err)
}

func TestStreamContextHandlerInterceptor(t *testing.T) {
	h := func(srv interface{}, stream grpc.ServerStream) error {
		return nil
	}

	ms := MockedServerStream{ctx: context.TODO()}
	err := StreamContextHandlerInterceptor(nil, &ms, nil, h)

	require.NoError(t, err)
}

func TestStreamContextHandlerInterceptor_Canceled(t *testing.T) {
	h := func(srv interface{}, stream grpc.ServerStream) error {
		time.Sleep(100 * time.Second)
		return nil
	}
	ctx, cancel := context.WithCancel(context.TODO())
	cancel()

	ms := MockedServerStream{ctx: ctx}
	err := StreamContextHandlerInterceptor(nil, &ms, nil, h)
	require.ErrorIs(t, errors.New(ErrCanceled), err)
}

func TestStreamContextHandlerInterceptor_DeadLineExceeded(t *testing.T) {
	h := func(srv interface{}, stream grpc.ServerStream) error {
		time.Sleep(100 * time.Second)
		return nil
	}
	ctx, _ := context.WithTimeout(context.TODO(), time.Nanosecond*1)

	ms := MockedServerStream{ctx: ctx}
	err := StreamContextHandlerInterceptor(nil, &ms, nil, h)
	require.ErrorIs(t, errors.New(ErrDeadlineExceeded), err)
}

type MockedServerStream struct {
	ctx context.Context
}

func (ss *MockedServerStream) SetHeader(metadata.MD) error {
	return nil
}

func (ss *MockedServerStream) SendHeader(metadata.MD) error {
	return nil
}

func (ss *MockedServerStream) SetTrailer(metadata.MD) {}

func (ss *MockedServerStream) Context() context.Context {
	return ss.ctx
}

func (ss *MockedServerStream) SendMsg(m interface{}) error {
	return nil
}

func (ss *MockedServerStream) RecvMsg(m interface{}) error {
	return nil
}
