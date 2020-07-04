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
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/auth"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/test/bufconn"
)

const bufSize = 1024 * 1024

var lis *bufconn.Listener

func bufDialer(ctx context.Context, address string) (net.Conn, error) {
	return lis.Dial()
}

const clientDir = "./handlers_test_client_dir"

var username string
var plainPass string

func newServer() *server.ImmuServer {
	is := server.DefaultServer()
	is = is.WithOptions(
		is.Options.
			WithAuth(true).
			WithInMemoryStore(true).
			WithCorruptionCheck(false).
			WithMetricsServer(false))
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

func newClient(withToken bool, token string) client.ImmuClient {
	os.RemoveAll(clientDir)
	if err := os.Mkdir(clientDir, 0755); err != nil {
		log.Fatalf("error creating client dir %s: %v", clientDir, err)
	}
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}
	if withToken {
		dialOptions = append(
			dialOptions,
			grpc.WithUnaryInterceptor(auth.ClientUnaryInterceptor(token)),
			grpc.WithStreamInterceptor(auth.ClientStreamInterceptor(token)),
		)
	}
	immuclient := client.DefaultClient().WithOptions(
		client.DefaultOptions().WithAuth(withToken).WithDialOptions(&dialOptions))
	clientConn, _ := immuclient.Connect(context.TODO())
	immuclient.WithClientConn(clientConn)
	serviceClient := schema.NewImmuServiceClient(clientConn)
	immuclient.WithServiceClient(serviceClient)
	rootService := client.NewRootService(
		serviceClient,
		cache.NewFileCache(clientDir),
		logger.NewSimpleLogger("handlers_test", os.Stdout))
	immuclient.WithRootService(rootService)
	return immuclient
}

type ntpMock struct {
	t time.Time
}

func (n *ntpMock) Now() time.Time {
	return n.t
}
func newNtpMock() (timestamp.TsGenerator, error) {
	i, err := strconv.ParseInt("1405544146", 10, 64)
	if err != nil {
		log.Fatal(err)
	}
	tm := time.Unix(i, 0)
	return &ntpMock{tm}, nil
}

func cleanup() {
	if err := os.RemoveAll(clientDir); err != nil {
		log.Println(err)
	}
}

func login() string {
	ic := newClient(false, "")
	ctx := context.Background()
	r, err := ic.Login(ctx, []byte(username), []byte(plainPass))
	if err != nil {
		log.Fatal(err)
	}
	return string(r.GetToken())
}

func TestGw(t *testing.T) {
	cleanup()
	defer cleanup()
	immuServer := newServer()
	immuServer.Start()
	nm, _ := newNtpMock()
	token := login()
	tss := client.NewTimestampService(nm)
	immuClient := newClient(true, token).WithTimestampService(tss)
	resp, err := immuClient.UseDatabase(context.Background(), &schema.Database{
		Databasename: immuServer.Options.GetDefaultDbName(),
	})
	if err != nil {
		panic(err)
	}
	immuClient = newClient(true, resp.Token).WithTimestampService(tss)
	require.NoError(t, immuClient.HealthCheck(context.Background()))
	mux := runtime.NewServeMux()
	testSafeSetHandler(t, NewSafesetHandler(mux, immuClient))
	testSetHandler(t, NewSetHandler(mux, immuClient))
	testSafeGetHandler(t, NewSafegetHandler(mux, immuClient))
	testHistoryHandler(t, NewHistoryHandler(mux, immuClient))
	testSafeReferenceHandler(t, NewSafeReferenceHandler(mux, immuClient))
	testSafeZAddHandler(t, NewSafeZAddHandler(mux, immuClient))
}

func testHandler(
	t *testing.T,
	name string,
	method string,
	path string,
	body string,
	handlerFunc func(http.ResponseWriter, *http.Request),
	testFunc func(*testing.T, string, int, map[string]interface{}),
) error {
	w := httptest.NewRecorder()
	req, _ := http.NewRequest(method, path, strings.NewReader(body))
	req.Header.Add("Content-Type", "application/json")
	handler := http.HandlerFunc(handlerFunc)
	handler.ServeHTTP(w, req)
	testCase := fmt.Sprintf("%s - %s %s %s - ", name, method, path, body)
	respBytes := w.Body.Bytes()
	var respBody map[string]interface{}
	if err := json.Unmarshal(respBytes, &respBody); err != nil {
		return fmt.Errorf(
			"%s - error unmarshaling JSON from response %s", testCase, respBytes)
	}
	testFunc(t, testCase, w.Code, respBody)
	return nil
}

func requireResponseStatus(
	t *testing.T,
	testCase string,
	expected int,
	actual int,
) {
	require.Equal(
		t,
		expected,
		actual,
		"%sexpected HTTP status %d, actual %d", testCase, expected, actual)
}

func getMissingResponseFieldPattern(testCase string) string {
	return testCase + "\"%s\" field is missing from response %v"
}

func requireResponseFields(
	t *testing.T,
	testCase string,
	fields []string,
	body map[string]interface{},
) {
	missingPattern := getMissingResponseFieldPattern(testCase)
	for _, field := range fields {
		_, ok := body[field]
		require.True(t, ok, missingPattern, field, body)
	}
}

func requireResponseFieldsTrue(
	t *testing.T,
	testCase string,
	fields []string,
	body map[string]interface{},
) {
	missingPattern := getMissingResponseFieldPattern(testCase)
	isFalsePattern := testCase + "\"%s\" field is false in response %v"
	for _, field := range fields {
		fieldValue, ok := body[field]
		require.True(t, ok, missingPattern, field, body)
		require.True(t, fieldValue.(bool), isFalsePattern, field, body)
	}
}

func requireResponseFieldsEqual(
	t *testing.T,
	testCase string,
	fields map[string]interface{},
	body map[string]interface{},
) {
	missingPattern := getMissingResponseFieldPattern(testCase)
	notEqPattern := testCase +
		"expected response %v to have field \"%s\" = \"%v\", but actual field value is \"%v\""
	for field, expected := range fields {
		fieldValue, ok := body[field]
		require.True(t, ok, missingPattern, field, body)
		require.Equal(
			t, expected, fieldValue, notEqPattern, body, field, expected, fieldValue)
	}
}
