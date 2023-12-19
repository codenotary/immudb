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

package state

/*
import (
	"context"
	"errors"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/clienttest"
	"github.com/codenotary/immudb/pkg/client/rootservice"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
)

func TestImmudbUUIDProvider_CurrentUuidNotFound(t *testing.T) {
	cli := &clienttest.ImmuServiceClientMock{}
	cli.HealthF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
		return &schema.HealthResponse{
			Status:  true,
			Version: "mock",
		}, nil
	}
	uuidp := rootservice.NewImmudbUUIDProvider(cli)
	uuid, err := uuidp.CurrentUUID(context.Background())
	assert.EqualError(t, err, "!IMPORTANT WARNING: immudb-uuid header is not published by the immudb server; this client MUST NOT be used to connect to different immudb servers!")
	assert.Equal(t, "", uuid)
}

func TestImmudbUUIDProvider_CurrentHealthError(t *testing.T) {
	cli := &clienttest.ImmuServiceClientMock{}
	cli.HealthF = func(ctx context.Context, in *empty.Empty, opts ...grpc.CallOption) (*schema.HealthResponse, error) {
		return nil, errors.New("mock")
	}
	uuidp := rootservice.NewImmudbUUIDProvider(cli)
	uuid, err := uuidp.CurrentUUID(context.Background())
	assert.Error(t, err)
	assert.Equal(t, "", uuid)
}
*/
