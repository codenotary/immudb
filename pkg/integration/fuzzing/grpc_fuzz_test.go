//go:build go1.18
// +build go1.18

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

package fuzzing

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	immudb "github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	requestExecAll = 0
	requestSet     = 1
)

func addCorpus(f *testing.F, request byte, msg proto.Message) {
	b, err := proto.Marshal(msg)
	require.NoError(f, err)
	f.Add(append([]byte{request}, b...))
}

func FuzzGRPCProtocol(f *testing.F) {
	options := server.DefaultOptions().WithDir(f.TempDir())
	bs := servertest.NewBufconnServer(options)

	bs.Start()
	defer bs.Stop()

	clientOpts := immudb.
		DefaultOptions().
		WithDir(f.TempDir()).
		WithDialOptions([]grpc.DialOption{grpc.WithContextDialer(bs.Dialer), grpc.WithTransportCredentials(insecure.NewCredentials())})
	client := immudb.NewClient().WithOptions(clientOpts)

	err := client.OpenSession(context.Background(), []byte(`immudb`), []byte(`immudb`), "defaultdb")
	require.NoError(f, err)

	// Add few execall requests
	addCorpus(f, requestExecAll, &schema.ExecAllRequest{Operations: []*schema.Op{{
		Operation: &schema.Op_Kv{
			Kv: &schema.KeyValue{
				Key:   []byte("key"),
				Value: []byte("value"),
			},
		},
	}}})
	addCorpus(f, requestExecAll, &schema.ExecAllRequest{Operations: []*schema.Op{{
		Operation: &schema.Op_Ref{
			Ref: &schema.ReferenceRequest{
				Key:           []byte("ref"),
				ReferencedKey: []byte("key"),
			},
		},
	}}})

	addCorpus(f, requestExecAll, &schema.ExecAllRequest{
		Operations: []*schema.Op{
			{
				Operation: &schema.Op_Ref{
					Ref: &schema.ReferenceRequest{
						Key:           []byte("ref"),
						ReferencedKey: []byte("key"),
					},
				},
			},
		},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustExist([]byte("key1")),
		},
	})

	addCorpus(f, requestSet, &schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
	})

	addCorpus(f, requestSet, &schema.SetRequest{
		KVs: []*schema.KeyValue{{
			Key:   []byte("key"),
			Value: []byte("value"),
		}},
		Preconditions: []*schema.Precondition{
			schema.PreconditionKeyMustNotExist([]byte("key-does-not-exist")),
		},
	})

	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) < 1 {
			t.Skip()
		}

		switch data[0] {
		case requestExecAll:

			req := &schema.ExecAllRequest{}
			err := proto.Unmarshal(data[1:], req)
			if err != nil {
				t.Skip()
			}

			client.ExecAll(context.Background(), req)

		case requestSet:

			req := &schema.SetRequest{}
			err := proto.Unmarshal(data[1:], req)
			if err != nil {
				t.Skip()
			}

			client.SetAll(context.Background(), req)

		default:
			t.Skip()
		}

	})

}
