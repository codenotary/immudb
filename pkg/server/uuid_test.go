/*
Copyright 2022 Codenotary Inc. All rights reserved.

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
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"testing"

	"github.com/rs/xid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func TestNewUUID(t *testing.T) {
	dir := t.TempDir()
	id, err := getOrSetUUID(dir, filepath.Join(dir, "defaultDb"), false)
	require.NoError(t, err)
	require.FileExists(t, filepath.Join(dir, IDENTIFIER_FNAME))

	uuid := NewUUIDContext(id)
	require.Equal(t, uuid.UUID, id)
}

// test for external identifier and remote storage
func TestNoUUID(t *testing.T) {
	dir := t.TempDir()
	id, err := getOrSetUUID(dir, filepath.Join(dir, "defaultDb"), true)
	require.NoError(t, err)
	require.Equal(t, xid.ID{}, id)
}

func TestExistingUUID(t *testing.T) {
	x, _ := xid.FromString("bs6c1kn1lu5qfesu061g")
	dir := t.TempDir()
	ioutil.WriteFile(filepath.Join(dir, IDENTIFIER_FNAME), x.Bytes(), os.ModePerm)
	id, err := getOrSetUUID(dir, filepath.Join(dir, "defaultDb"), false)
	require.NoError(t, err)

	require.FileExists(t, filepath.Join(dir, IDENTIFIER_FNAME))

	uuid := NewUUIDContext(id)
	require.Equal(t, uuid.UUID, id)
}

func TestMigrateUUID(t *testing.T) {
	dir := t.TempDir()
	defaultDbDir := filepath.Join(dir, "defaultDb")
	err := os.Mkdir(defaultDbDir, os.ModePerm)
	require.NoError(t, err)

	fileInDefaultDbDir := path.Join(defaultDbDir, IDENTIFIER_FNAME)
	x, _ := xid.FromString("bs6c1kn1lu5qfesu061g")
	ioutil.WriteFile(fileInDefaultDbDir, x.Bytes(), os.ModePerm)
	id, err := getOrSetUUID(dir, defaultDbDir, false)
	require.NoError(t, err)

	require.FileExists(t, filepath.Join(dir, IDENTIFIER_FNAME))
	require.NoFileExists(t, fileInDefaultDbDir, "uuid file not moved, %s", err)

	uuid := NewUUIDContext(id)
	require.Equal(t, id, uuid.UUID)
}

func TestUUIDContextSetter(t *testing.T) {
	dir := t.TempDir()
	id, err := getOrSetUUID(dir, filepath.Join(dir, "defaultDb"), false)
	require.NoError(t, err)

	uuid := NewUUIDContext(id)
	transportStream := &mockServerTransportStream{}
	srv := &grpc.UnaryServerInfo{}

	handler := func(ctx context.Context, req interface{}) (interface{}, error) {

		ctxUUID, ok := transportStream.SentHeader[SERVER_UUID_HEADER]
		require.True(t, ok, "error setting uuid")

		x, err := xid.FromString(ctxUUID[0])
		require.NoError(t, err)
		require.Equal(t, uuid.UUID, x)

		return req, nil
	}

	var req interface{}
	ctx := grpc.NewContextWithServerTransportStream(context.Background(), transportStream)

	_, err = uuid.UUIDContextSetter(ctx, req, srv, handler)
	require.NoError(t, err)
}

func TestUUIDStreamContextSetter(t *testing.T) {
	dir := t.TempDir()
	id, err := getOrSetUUID(dir, filepath.Join(dir, "defaultDb"), false)
	require.NoError(t, err)

	uuid := NewUUIDContext(id)
	srv := grpc.StreamServerInfo{}
	ss := mockServerStream{}

	handler := func(srv interface{}, stream grpc.ServerStream) error {
		ctxUUID, ok := ss.SentHeader[SERVER_UUID_HEADER]
		require.True(t, ok, "error setting uuid")

		x, err := xid.FromString(ctxUUID[0])
		require.NoError(t, err)
		require.Equal(t, uuid.UUID, x)

		return nil
	}

	var req interface{}

	err = uuid.UUIDStreamContextSetter(req, &ss, &srv, handler)
	require.NoError(t, err)
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
