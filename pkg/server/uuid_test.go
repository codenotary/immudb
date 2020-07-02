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

package server

import (
	"context"
	"os"
	"testing"

	"github.com/rs/xid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestNewUUID(t *testing.T) {
	id, err := getOrSetUuid("./")
	if err != nil {
		t.Fatalf("error creating UUID, %v", err)
	}
	defer os.RemoveAll(IDENTIFIER_FNAME)

	if !fileExists(IDENTIFIER_FNAME) {
		t.Errorf("uuid file not created, %s", err)
	}

	uuid := NewUuidContext(id)
	if id.Compare(uuid.Uuid) != 0 {
		t.Fatalf("NewUuidContext error expected %v, got %v", id, uuid.Uuid)
	}
}

func TestUuidContextSetter(t *testing.T) {
	id, err := getOrSetUuid("./")
	if err != nil {
		t.Fatalf("error creating UUID, %v", err)
	}
	defer os.RemoveAll(IDENTIFIER_FNAME)
	uuid := NewUuidContext(id)
	transportStream := &mockServerTransportStream{}
	srv := &grpc.UnaryServerInfo{}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {

		ctxUUID, ok := transportStream.SentHeader[SERVER_UUID_HEADER]
		if !ok {
			t.Fatalf("error setting uuid")
		}

		x, err := xid.FromString(ctxUUID[0])
		if err != nil {
			t.Fatalf("error initializing xid from string %s", ctxUUID[0])
		}
		if uuid.Uuid.Compare(x) != 0 {
			t.Fatalf("set uuid does is not equal to transmited uuid")
		}
		return req, nil
	}
	var req interface{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), transportStream)
	_, err = uuid.UuidContextSetter(ctx, req, srv, handler)
	if err != nil {
		t.Fatalf("error setting uuid UUID, %v", err)
	}
}
func TestUuidStreamContextSetter(t *testing.T) {
	id, err := getOrSetUuid("./")
	if err != nil {
		t.Fatalf("error creating UUID, %v", err)
	}
	defer os.RemoveAll(IDENTIFIER_FNAME)
	uuid := NewUuidContext(id)
	srv := grpc.StreamServerInfo{}
	ss := mockServerStream{}
	handler := func(srv interface{}, stream grpc.ServerStream) error {
		ctxUUID, ok := ss.SentHeader[SERVER_UUID_HEADER]
		if !ok {
			t.Fatalf("error setting uuid")
		}
		x, err := xid.FromString(ctxUUID[0])
		if err != nil {
			t.Fatalf("error initializing xid from string %s", ctxUUID[0])
		}
		if uuid.Uuid.Compare(x) != 0 {
			t.Fatalf("set uuid does is not equal to transmited uuid")
		}
		return nil
	}
	var req interface{}
	err = uuid.UuidStreamContextSetter(req, &ss, &srv, handler)
	if err != nil {
		t.Fatalf("error setting uuid UUID, %v", err)
	}
}

// implement ServerTransportStream
type mockServerTransportStream struct {
	SentHeader metadata.MD
}

func (r *mockServerTransportStream) Method() string                  { return "" }
func (r *mockServerTransportStream) SetHeader(md metadata.MD) error  { return nil }
func (r *mockServerTransportStream) SendHeader(md metadata.MD) error { r.SentHeader = md; return nil }
func (r *mockServerTransportStream) SetTrailer(md metadata.MD) error { return nil }

// implement ServerStream
type mockServerStream struct {
	SentHeader metadata.MD
	ctx        context.Context
}

func (r *mockServerStream) SetHeader(md metadata.MD) error  { return nil }
func (r *mockServerStream) SendHeader(md metadata.MD) error { r.SentHeader = md; return nil }
func (r *mockServerStream) SetTrailer(md metadata.MD)       {}
func (r *mockServerStream) Context() context.Context        { return r.ctx }
func (r *mockServerStream) SendMsg(m interface{}) error     { return nil }
func (r *mockServerStream) RecvMsg(m interface{}) error     { return nil }
