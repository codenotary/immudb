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
	errSafeGet := errors.New("SafeGetF got called")
	errSafeSet := errors.New("SafeSetF got called")
	errSet := errors.New("SetF got called")
	errSafeReference := errors.New("SafeReferenceF got called")
	errSafeZAdd := errors.New("SafeZAddF got called")
	errHistory := errors.New("HistoryF got called")
	icm := &ImmuClientMock{
		ImmuClient: client.DefaultClient(),
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
		SafeGetF: func(context.Context, []byte, ...grpc.CallOption) (*client.VerifiedItem, error) {
			return nil, errSafeGet
		},
		SafeSetF: func(context.Context, []byte, []byte) (*client.VerifiedIndex, error) {
			return nil, errSafeSet
		},
		SetF: func(context.Context, []byte, []byte) (*schema.Index, error) {
			return nil, errSet
		},
		SafeReferenceF: func(context.Context, []byte, []byte) (*client.VerifiedIndex, error) {
			return nil, errSafeReference
		},
		SafeZAddF: func(context.Context, []byte, float64, []byte) (*client.VerifiedIndex, error) {
			return nil, errSafeZAdd
		},
		HistoryF: func(context.Context, []byte) (*schema.StructuredItemList, error) {
			return nil, errHistory
		},
	}
	require.True(t, icm.IsConnected())

	require.Equal(t, errWaitForHealthCheck, icm.WaitForHealthCheck(nil))
	_, err := icm.Connect(nil)

	require.Equal(t, errConnect, err)
	err = icm.Disconnect()

	require.Equal(t, errDisconnect, err)
	_, err = icm.Login(nil, nil, nil)

	require.Equal(t, errLogin, err)

	require.Equal(t, errLogout, icm.Logout(nil))
	_, err = icm.SafeGet(nil, nil)

	require.Equal(t, errSafeGet, err)
	_, err = icm.SafeSet(nil, nil, nil)

	require.Equal(t, errSafeSet, err)
	_, err = icm.Set(nil, nil, nil)

	require.Equal(t, errSet, err)
	_, err = icm.SafeReference(nil, nil, nil)

	require.Equal(t, errSafeReference, err)
	_, err = icm.SafeZAdd(nil, nil, 0., nil)

	require.Equal(t, errSafeZAdd, err)
	_, err = icm.History(nil, nil)

	require.Equal(t, errHistory, err)
}
