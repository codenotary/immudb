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
	"errors"
	"fmt"
	"google.golang.org/grpc/metadata"
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
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/timestamp"
	"github.com/codenotary/immudb/pkg/json"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func newClient(bufDialer func(context.Context, string) (net.Conn, error)) client.ImmuClient {
	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bufDialer), grpc.WithInsecure(),
	}
	immuclient := client.DefaultClient().WithOptions(
		client.DefaultOptions().WithAuth(false).WithDialOptions(&dialOptions))
	clientConn, _ := immuclient.Connect(context.TODO())
	immuclient.WithClientConn(clientConn)
	serviceClient := schema.NewImmuServiceClient(clientConn)
	immuclient.WithServiceClient(serviceClient)
	rootService := client.NewRootService(
		serviceClient,
		cache.NewInMemoryCache(),
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

func TestGw(t *testing.T) {
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(false).WithInMemoryStore(true))
	bs.Start()
	nm, _ := newNtpMock()
	immuClient := newClient(bs.Dialer).WithTimestampService(client.NewTimestampService(nm))
	require.NoError(t, immuClient.HealthCheck(context.Background()))
	mux := runtime.NewServeMux()
	testSafeSetHandler(t, mux, immuClient)
	testSetHandler(t, mux, immuClient)
	testSafeGetHandler(t, mux, immuClient)
	testHistoryHandler(t, mux, immuClient)
	testSafeReferenceHandler(t, mux, immuClient)
	testSafeZAddHandler(t, mux, immuClient)
}

func TestAuthGw(t *testing.T) {
	bs := servertest.NewBufconnServer(server.Options{}.WithAuth(true).WithInMemoryStore(true))
	bs.Start()
	nm, _ := newNtpMock()
	immuClient := newClient(bs.Dialer).WithTimestampService(client.NewTimestampService(nm))

	ctx := context.Background()

	dialOptions := []grpc.DialOption{
		grpc.WithContextDialer(bs.Dialer), grpc.WithInsecure(),
	}
	pr := &PasswordReader{
		Pass: []string{"immudb"},
	}
	ts := client.NewTokenService().WithTokenFileName("testTokenFile").WithHds(client.NewHomedirService())
	cliopt := client.DefaultOptions().WithDialOptions(&dialOptions).WithPasswordReader(pr).WithTokenService(ts)
	cliopt.PasswordReader = pr
	cliopt.DialOptions = &dialOptions

	cli, _ := client.NewImmuClient(cliopt)
	lresp, err := cli.Login(ctx, []byte("immudb"), []byte("immudb"))
	if err != nil {
		t.Fatal(err)
	}

	md := metadata.Pairs("authorization", lresp.Token)
	ctx = metadata.NewOutgoingContext(context.Background(), md)

	require.NoError(t, immuClient.HealthCheck(ctx))
	mux := runtime.NewServeMux()
	testUseDatabaseHandler(t, ctx, mux, immuClient)
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
	if err := json.DefaultJSON().Unmarshal(respBytes, &respBody); err != nil {
		return fmt.Errorf(
			"%s - error unmarshaling JSON from response %s", testCase, respBytes)
	}
	testFunc(t, testCase, w.Code, respBody)
	return nil
}

func newTestJSONWithMarshalErr() json.JSON {
	return &json.StandardJSON{
		MarshalF: func(v interface{}) ([]byte, error) {
			return nil, errors.New("JSON marshal error")
		},
	}
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

type PasswordReader struct {
	Pass       []string
	callNumber int
}

func (pr *PasswordReader) Read(msg string) ([]byte, error) {
	if len(pr.Pass) <= pr.callNumber {
		log.Fatal("Application requested the password more times than number of passwords supplied")
	}
	pass := []byte(pr.Pass[pr.callNumber])
	pr.callNumber++
	return pass, nil
}
