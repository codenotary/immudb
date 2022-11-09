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

package immuc

/*
import (
	"context"
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMiscErrors(t *testing.T) {
	immuClientMock := &clienttest.ImmuClientMock{}
	ic := &immuc{ImmuClient: immuClientMock}

	// History errors
	args := []string{"key1"}
	immuClientMock.HistoryF = func(context.Context, *schema.HistoryOptions) (*schema.StructuredItemList, error) {
		return nil, status.New(codes.Internal, "history RPC error").Err()
	}
	resp, err := ic.History(args)
	require.NoError(t, err)
	require.Equal(t, " history RPC error", resp)

	errHistory := errors.New("history error")
	immuClientMock.HistoryF = func(context.Context, *schema.HistoryOptions) (*schema.StructuredItemList, error) {
		return nil, errHistory
	}
	_, err = ic.History(args)
	require.ErrorIs(t, err, errHistory)

	// HealthCheck errors
	immuClientMock.HealthCheckF = func(context.Context) error {
		return status.New(codes.Internal, "health check RPC error").Err()
	}
	resp, err = ic.HealthCheck(nil)
	require.NoError(t, err)
	require.Equal(t, " health check RPC error", resp)

	errHealthCheck := errors.New("health check error")
	immuClientMock.HealthCheckF = func(context.Context) error {
		return errHealthCheck
	}
	_, err = ic.HealthCheck(nil)
	require.ErrorIs(t, err, errHealthCheck)
}
*/
