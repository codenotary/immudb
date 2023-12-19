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

import (
	"context"
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc"
)

// SERVER_UUID_HEADER ...
const SERVER_UUID_HEADER = "immudb-uuid"

// ErrNoServerUuid ...
var ErrNoServerUuid = fmt.Errorf(
	"!IMPORTANT WARNING: %s header is not published by the immudb server; "+
		"this client MUST NOT be used to connect to different immudb servers!",
	SERVER_UUID_HEADER)

type UUIDProvider interface {
	CurrentUUID(ctx context.Context) (string, error)
}

type uuidProvider struct {
	client schema.ImmuServiceClient
}

func NewUUIDProvider(client schema.ImmuServiceClient) UUIDProvider {
	return &uuidProvider{client}
}

// CurrentUUID issues a Health command to the server, then parses and returns
// the server UUID from the response metadata
func (r *uuidProvider) CurrentUUID(ctx context.Context) (string, error) {
	var metadata runtime.ServerMetadata
	if _, err := r.client.Health(
		ctx,
		new(empty.Empty),
		grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD),
	); err != nil {
		return "", err
	}
	var serverUUID string
	if len(metadata.HeaderMD.Get(SERVER_UUID_HEADER)) > 0 {
		serverUUID = metadata.HeaderMD.Get(SERVER_UUID_HEADER)[0]
	}
	if serverUUID == "" {
		return "", ErrNoServerUuid
	}
	return serverUUID, nil
}
