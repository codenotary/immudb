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
	errVerifiableGet := errors.New("VerifiableGetF got called")
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
		VerifiableGetF: func(ctx context.Context, in *schema.VerifiableGetRequest, opts ...grpc.CallOption) (*schema.VerifiableEntry, error) {
			return nil, errVerifiableGet
		},
		SetF: func(context.Context, []byte, []byte) (*schema.TxHeader, error) {
			return nil, errSet
		},
		SetAllF: func(context.Context, *schema.SetRequest) (*schema.TxHeader, error) {
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

	err := icm.WaitForHealthCheck(context.Background())
	require.ErrorIs(t, err, errWaitForHealthCheck)

	_, err = icm.Connect(context.Background())
	require.ErrorIs(t, err, errConnect)

	err = icm.Disconnect()
	require.ErrorIs(t, err, errDisconnect)

	_, err = icm.Login(context.Background(), nil, nil)
	require.ErrorIs(t, err, errLogin)

	require.ErrorIs(t, errLogout, icm.Logout(context.Background()))

	_, err = icm.VerifiedGet(context.Background(), nil)
	require.ErrorIs(t, err, errVerifiedGet)

	_, err = icm.VerifiedSet(context.Background(), nil, nil)
	require.ErrorIs(t, err, errVerifiedSet)

	_, err = icm.VerifiableGet(context.Background(), nil, nil)
	require.ErrorIs(t, err, errVerifiableGet)

	_, err = icm.Set(context.Background(), nil, nil)
	require.ErrorIs(t, err, errSet)

	_, err = icm.SetAll(context.Background(), nil)
	require.ErrorIs(t, err, errSet)

	_, err = icm.VerifiedSetReference(context.Background(), nil, nil)
	require.ErrorIs(t, err, errVerifiedReference)

	_, err = icm.VerifiedZAdd(context.Background(), nil, 0., nil)
	require.ErrorIs(t, err, errVerifiedZAdd)

	_, err = icm.History(context.Background(), nil)
	require.ErrorIs(t, err, errHistory)

	_, err = icm.CreateDatabaseV2(context.Background(), "", nil)
	require.ErrorIs(t, err, errCreateDatabase)
}
