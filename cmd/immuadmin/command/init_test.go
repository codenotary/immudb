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

package immuadmin

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/spf13/viper"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
	"log"
	"net"
	"testing"
)

func TestOptions(t *testing.T) {
	opts := Options()
	assert.IsType(t, &client.Options{}, opts)
}

func TestOptionsMtls(t *testing.T) {
	viper.Set("mtls", true)
	opts := Options()
	assert.IsType(t, &client.Options{}, opts)
	viper.Reset()
}

const bufSize = 1024 * 1024

var lis *bufconn.Listener

var immuServer *server.ImmuServer
var cli client.ImmuClient
var username string
var plainPass string

func newServer(authRequired bool) *server.ImmuServer {
	is := server.DefaultServer()
	is = is.WithOptions(is.Options.WithAuth(authRequired).WithInMemoryStore(true))
	auth.AuthEnabled = is.Options.GetAuth()

	username, plainPass = auth.SysAdminUsername, auth.SysAdminPassword

	lis = bufconn.Listen(bufSize)
	s := grpc.NewServer(
		grpc.UnaryInterceptor(auth.ServerUnaryInterceptor),
		grpc.StreamInterceptor(auth.ServerStreamInterceptor),
	)
	schema.RegisterImmuServiceServer(s, is)
	go func() {
		if err := s.Serve(lis); err != nil {
			log.Fatal(err)
		}
	}()
	return is
}

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}
