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

package rootservice_test

import (
	"context"
	"errors"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/client/rootservice"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
)

func TestImmudbUuidProvider_CurrentUuidNotFound(t *testing.T) {
	cli := &clienttest.ImmuServiceClientMock{}
	cli.HealthF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
		return &schema.HealthResponse{
			Status:  true,
			Version: "mock",
		}, nil
	}
	uuidp := rootservice.NewImmudbUuidProvider(cli)
	uuid, err := uuidp.CurrentUuid(context.Background())
	assert.EqualError(t, err, "!IMPORTANT WARNING: immudb-uuid header is not published by the immudb server; this client MUST NOT be used to connect to different immudb servers!")
	assert.Equal(t, "", uuid)
}

func TestImmudbUuidProvider_CurrentHealtError(t *testing.T) {
	cli := &clienttest.ImmuServiceClientMock{}
	cli.HealthF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
		return nil, errors.New("mock")
	}
	uuidp := rootservice.NewImmudbUuidProvider(cli)
	uuid, err := uuidp.CurrentUuid(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "", uuid)
}
