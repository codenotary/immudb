/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package auditor

import (
	"context"
	"crypto/ecdsa"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/codenotary/immudb/pkg/signer"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

var dirname = "./test"

func TestDefaultAuditor(t *testing.T) {
	defer os.RemoveAll(dirname)
	da, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		[]grpc.DialOption{},
		"immudb",
		"immudb",
		nil,
		nil,
		AuditNotificationConfig{},
		nil,
		nil,
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil)
	require.NoError(t, err)
	require.IsType(t, &defaultAuditor{}, da)
}

type writerMock struct {
	written []string
}

func (wm *writerMock) Write(bs []byte) (n int, err error) {
	wm.written = append(wm.written, string(bs))
	return len(bs), nil
}

func TestDefaultAuditorPasswordDecodeErr(t *testing.T) {
	defer os.RemoveAll(dirname)
	_, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		[]grpc.DialOption{},
		"immudb",
		"enc:"+string([]byte{0}),
		nil,
		nil,
		AuditNotificationConfig{},
		nil,
		nil,
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil)
	require.ErrorContains(t, err, "illegal base64 data at input byte 0")
}

func TestDefaultAuditorLoginErr(t *testing.T) {
	defer os.RemoveAll(dirname)

	serviceClient := clienttest.ImmuServiceClientMock{
		LoginF: func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
			return nil, errors.New("some login error")
		},
		LogoutF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
			return new(empty.Empty), nil
		},
	}

	wm := writerMock{}
	auditor, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		"immudb",
		"immudb",
		nil,
		nil,
		AuditNotificationConfig{},
		&serviceClient,
		state.NewUUIDProvider(&serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", &wm),
		nil)
	require.NoError(t, err)
	err = auditor.(*defaultAuditor).audit()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(wm.written), 1)
	require.Contains(t, wm.written[len(wm.written)-1], "some login error")
}

func TestDefaultAuditorDatabaseListErr(t *testing.T) {
	defer os.RemoveAll(dirname)
	serviceClient := clienttest.ImmuServiceClientMock{
		LoginF: func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
			return &schema.LoginResponse{Token: ""}, nil
		},
		LogoutF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
			return new(empty.Empty), nil
		},
		DatabaseListF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
			return nil, errors.New("some database list error")
		},
	}
	wm := writerMock{}
	auditor, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		"immudb",
		"immudb",
		nil,
		nil,
		AuditNotificationConfig{},
		&serviceClient,
		state.NewUUIDProvider(&serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", &wm),
		nil)
	require.NoError(t, err)
	err = auditor.(*defaultAuditor).audit()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(wm.written), 1)
	require.Contains(t, wm.written[len(wm.written)-1], "some database list error")
}

func TestDefaultAuditorDatabaseListEmpty(t *testing.T) {
	defer os.RemoveAll(dirname)
	serviceClient := clienttest.ImmuServiceClientMock{
		LoginF: func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
			return &schema.LoginResponse{Token: ""}, nil
		},
		LogoutF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
			return new(empty.Empty), nil
		},
		DatabaseListF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
			return &schema.DatabaseListResponse{
				Databases: nil,
			}, nil
		},
	}
	wm := writerMock{}
	auditor, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		"immudb",
		"immudb",
		nil,
		nil,
		AuditNotificationConfig{},
		&serviceClient,
		state.NewUUIDProvider(&serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", &wm),
		nil)
	require.NoError(t, err)
	err = auditor.(*defaultAuditor).audit()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(wm.written), 1)
	require.Contains(t, wm.written[len(wm.written)-1], "no databases to audit found")
}

func TestDefaultAuditorUseDatabaseErr(t *testing.T) {
	defer os.RemoveAll(dirname)
	serviceClient := clienttest.ImmuServiceClientMock{
		LoginF: func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
			return &schema.LoginResponse{Token: ""}, nil
		},
		LogoutF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
			return new(empty.Empty), nil
		},
		DatabaseListF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
			return &schema.DatabaseListResponse{
				Databases: []*schema.Database{{DatabaseName: "someDB"}},
			}, nil
		},
		UseDatabaseF: func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error) {
			return nil, errors.New("some use database error")
		},
	}
	wm := writerMock{}
	auditor, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		"immudb",
		"immudb",
		nil,
		nil,
		AuditNotificationConfig{},
		&serviceClient,
		state.NewUUIDProvider(&serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", &wm),
		nil)
	require.NoError(t, err)
	err = auditor.(*defaultAuditor).audit()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(wm.written), 1)
	require.Contains(t, wm.written[len(wm.written)-1], "some use database error")
}

func TestDefaultAuditorCurrentRootErr(t *testing.T) {
	defer os.RemoveAll(dirname)
	serviceClient := clienttest.ImmuServiceClientMock{
		LoginF: func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
			return &schema.LoginResponse{Token: ""}, nil
		},
		LogoutF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
			return new(empty.Empty), nil
		},
		DatabaseListF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
			return &schema.DatabaseListResponse{
				Databases: []*schema.Database{{DatabaseName: "someDB"}},
			}, nil
		},
		UseDatabaseF: func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error) {
			return &schema.UseDatabaseReply{Token: ""}, nil
		},
		CurrentStateF: func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.ImmutableState, error) {
			return nil, errors.New("some current state error")
		},
	}
	wm := writerMock{}
	auditor, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		"immudb",
		"immudb",
		nil,
		nil,
		AuditNotificationConfig{},
		&serviceClient,
		state.NewUUIDProvider(&serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", &wm),
		nil)
	require.NoError(t, err)
	err = auditor.(*defaultAuditor).audit()
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(wm.written), 1)
	require.Contains(t, wm.written[len(wm.written)-1], "some current state error")
}

func TestDefaultAuditorRunOnDbWithInvalidSignature(t *testing.T) {
	pk, err := signer.ParsePublicKeyFile("./../../../test/signer/ec1.pub")
	require.NoError(t, err)

	testDefaultAuditorRunOnDbWithInvalidSignature(t, pk, false)
	testDefaultAuditorRunOnDbWithInvalidSignature(t, pk, true)
}

func TestDefaultAuditorRunOnDbWithInvalidSignatureFromState(t *testing.T) {
	testDefaultAuditorRunOnDbWithInvalidSignature(t, nil, false)
	testDefaultAuditorRunOnDbWithInvalidSignature(t, nil, true)
}

func testDefaultAuditorRunOnDbWithInvalidSignature(t *testing.T, pk *ecdsa.PublicKey, withSignedState bool) {
	defer os.RemoveAll(dirname)

	serviceClient := &clienttest.ImmuServiceClientMock{}

	serviceClient.HealthF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
		return &schema.HealthResponse{Status: true, Version: "v1.0.0"}, nil
	}
	serviceClient.CurrentStateF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.ImmutableState, error) {
		currState := &schema.ImmutableState{}
		if withSignedState {
			currState.Signature = &schema.Signature{
				Signature: []byte("invalid signature"),
				PublicKey: []byte("invalid public key"),
			}
		}
		return currState, nil
	}
	serviceClient.LoginF = func(ctx context.Context, in *schema.LoginRequest, opts ...grpc.CallOption) (*schema.LoginResponse, error) {
		return &schema.LoginResponse{
			Token: "token",
		}, nil
	}
	serviceClient.DatabaseListF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.DatabaseListResponse, error) {
		return &schema.DatabaseListResponse{
			Databases: []*schema.Database{{DatabaseName: "sysdb"}},
		}, nil
	}
	serviceClient.UseDatabaseF = func(ctx context.Context, in *schema.Database, opts ...grpc.CallOption) (*schema.UseDatabaseReply, error) {
		return &schema.UseDatabaseReply{
			Token: "sometoken",
		}, nil
	}
	serviceClient.LogoutF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*empty.Empty, error) {
		return &empty.Empty{}, nil
	}

	da, err := DefaultAuditor(
		time.Duration(0),
		fmt.Sprintf("%s:%d", "address", 0),
		[]grpc.DialOption{
			grpc.WithTransportCredentials(insecure.NewCredentials()),
		},
		"immudb",
		"immudb",
		nil,
		pk,
		AuditNotificationConfig{},
		serviceClient,
		state.NewUUIDProvider(serviceClient),
		cache.NewHistoryFileCache(dirname),
		func(string, string, bool, bool, bool, *schema.ImmutableState, *schema.ImmutableState) {},
		logger.NewSimpleLogger("test", os.Stdout),
		nil)
	require.NoError(t, err)

	auditorDone := make(chan struct{}, 2)
	err = da.Run(time.Duration(10), true, context.Background().Done(), auditorDone)
	require.NoError(t, err)
	err = da.Run(time.Duration(10), true, context.Background().Done(), auditorDone)
	require.NoError(t, err)
}

func TestPublishAuditNotification(t *testing.T) {
	notificationConfig := AuditNotificationConfig{
		URL:      "http://some-non-existent-url.com",
		Username: "some-username",
		Password: "some-password",
		PublishFunc: func(req *http.Request) (*http.Response, error) {
			return &http.Response{
				Status:     http.StatusText(http.StatusNoContent),
				StatusCode: http.StatusNoContent,
				Body:       ioutil.NopCloser(strings.NewReader("All good")),
			}, nil
		},
	}
	a := &defaultAuditor{notificationConfig: notificationConfig}
	runAt, err := time.Parse(time.RFC3339, "2020-11-13T00:53:42+01:00")
	require.NoError(t, err)

	// test happy path
	err = a.publishAuditNotification(
		"some-db",
		runAt,
		true,
		&State{Tx: 1, Hash: "hash-1"},
		&State{Tx: 2, Hash: "hash-2"},
	)
	require.NoError(t, err)

	// test unexpected HTTP status code
	a.notificationConfig.PublishFunc = func(req *http.Request) (*http.Response, error) {
		return &http.Response{
			Status:     http.StatusText(http.StatusInternalServerError),
			StatusCode: http.StatusInternalServerError,
			Body:       ioutil.NopCloser(strings.NewReader("Some error")),
		}, nil
	}
	err = a.publishAuditNotification(
		"some-db2",
		runAt,
		false,
		&State{
			Tx:        11,
			Hash:      "hash-11",
			Signature: Signature{Signature: "sig11", PublicKey: "pk11"}},
		&State{
			Tx:        22,
			Hash:      "hash-22",
			Signature: Signature{Signature: "sig22", PublicKey: "pk22"}},
	)
	require.ErrorContains(
		t,
		err,
		"POST http://some-non-existent-url.com request with payload")
	require.ErrorContains(
		t,
		err,
		"got unexpected response status Internal Server Error with response body Some error")
	require.NotContains(t, err.Error(), notificationConfig.Password)

	// test error creating request
	a.notificationConfig.RequestTimeout = 1 * time.Second
	a.notificationConfig.URL = string([]byte{0})
	err = a.publishAuditNotification(
		"some-db4",
		runAt,
		true,
		&State{Tx: 1111, Hash: "hash-1111"},
		&State{Tx: 2222, Hash: "hash-2222"},
	)
	require.ErrorContains(t, err, "invalid control character in URL")
}
