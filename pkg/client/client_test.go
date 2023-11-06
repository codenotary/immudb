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

package client

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestLogErr(t *testing.T) {
	logger := logger.NewSimpleLogger("client_test", os.Stderr)

	require.Nil(t, logErr(logger, "error: %v", nil))

	err := fmt.Errorf("expected error")
	require.ErrorIs(t, logErr(logger, "error: %v", err), err)
}

func TestImmuClient_Truncate(t *testing.T) {
	c := NewClient().WithOptions(DefaultOptions().WithDir("false"))
	c.ServiceClient = &immuServiceClientMock{
		TruncateF: func(ctx context.Context, in *schema.TruncateDatabaseRequest, opts ...grpc.CallOption) (*schema.TruncateDatabaseResponse, error) {
			return &schema.TruncateDatabaseResponse{
				Database: "test",
			}, nil
		},
	}

	st := time.Now().Add(-24 * time.Hour)
	dur := time.Since(st)
	err := c.TruncateDatabase(context.Background(), "defaultdb", dur)
	require.Error(t, err)
}
