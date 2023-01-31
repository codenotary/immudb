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

package clienttest

import (
	"context"
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestImmuClientMock(t *testing.T) {
	errWaitForHealthCheck := errors.New("WaitForHealthCheckF got called")
	errConnect := errors.New("ConnectF got called")
	errDisconnect := errors.New("DisconnectF got called")
	errLogin := errors.New("LoginF got called")
	errLogout := errors.New("LogoutF got called")
	errVerifiedGet := errors.New("VerifiedGetF got called")
	errVerifiedSet := errors.New("VerifiedSetF got called")
	errSet := errors.New("SetF got called")
	errVerifiedReference := errors.New("VerifiedReferenceF got called")
	errVerifiedZAdd := errors.New("VerifiedZAddF got called")
	errHistory := errors.New("HistoryF got called")
	errCreateDatabase := errors.New("CreateDatabaseV2F got called")
	icm := &ImmuClientMock{
		ImmuClient: client.NewClient(),
		IsConnectedF: func() bool {
			return true
		},
		WaitForHealthCheckF: func(context.Context) error {
			return errWaitForHealthCheck
		},
		ConnectF: func(context.Context) (*grpc.ClientConn, error) {
			return nil, errConnect
		},
		DisconnectF: func() error {
			return errDisconnect
		},
		LoginF: func(context.Context, []byte, []byte) (*schema.LoginResponse, error) {
			return nil, errLogin
		},
		LogoutF: func(context.Context) error {
			return errLogout
		},
		VerifiedGetF: func(context.Context, []byte, ...client.GetOption) (*schema.Entry, error) {
			return nil, errVerifiedGet
		},
		VerifiedSetF: func(context.Context, []byte, []byte) (*schema.TxHeader, error) {
			return nil, errVerifiedSet
		},
		SetF: func(context.Context, []byte, []byte) (*schema.TxHeader, error) {
			return nil, errSet
		},
		VerifiedSetReferenceF: func(context.Context, []byte, []byte, uint64) (*schema.TxHeader, error) {
			return nil, errVerifiedReference
		},
		VerifiedZAddF: func(context.Context, []byte, float64, []byte, uint64) (*schema.TxHeader, error) {
			return nil, errVerifiedZAdd
		},
		HistoryF: func(context.Context, *schema.HistoryRequest) (*schema.Entries, error) {
			return nil, errHistory
		},
		CreateDatabaseV2F: func(context.Context, string, *schema.DatabaseNullableSettings) (*schema.CreateDatabaseResponse, error) {
			return nil, errCreateDatabase
		},
	}
	require.True(t, icm.IsConnected())

	require.Equal(t, errWaitForHealthCheck, icm.WaitForHealthCheck(context.Background()))
	_, err := icm.Connect(context.Background())

	require.Equal(t, errConnect, err)
	err = icm.Disconnect()

	require.Equal(t, errDisconnect, err)
	_, err = icm.Login(context.Background(), nil, nil)

	require.Equal(t, errLogin, err)

	require.Equal(t, errLogout, icm.Logout(context.Background()))
	_, err = icm.VerifiedGet(context.Background(), nil)

	require.Equal(t, errVerifiedGet, err)
	_, err = icm.VerifiedSet(context.Background(), nil, nil)

	require.Equal(t, errVerifiedSet, err)
	_, err = icm.Set(context.Background(), nil, nil)

	require.Equal(t, errSet, err)
	_, err = icm.VerifiedSetReference(context.Background(), nil, nil)

	require.Equal(t, errVerifiedReference, err)
	_, err = icm.VerifiedZAdd(context.Background(), nil, 0., nil)

	require.Equal(t, errVerifiedZAdd, err)
	_, err = icm.History(context.Background(), nil)

	require.Equal(t, errHistory, err)

	_, err = icm.CreateDatabaseV2(context.Background(), "", nil)
	require.Equal(t, errCreateDatabase, err)
}
