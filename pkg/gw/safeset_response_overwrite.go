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

package gw

import (
	"context"
	"encoding/json"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"net/http"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)


type SafeSetResponseOverwrite interface {
	call(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error)
}

type safeSetResponseOverwrite struct {
	rs client.RootService
}

func NewSafeSetResponseOverwrite(rs client.RootService) SafeSetResponseOverwrite{
	return safeSetResponseOverwrite{rs}
}

func (r safeSetResponseOverwrite) call (ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) {
	if req.Method == http.MethodPost && resp != nil && req.URL.Path == "/v1/immurestproxy/item/safe" {
		if p, ok := resp.(*schema.Proof); ok {
			proof := schema.Proof{p.Leaf, p.Index, p.Root, p.At, p.InclusionPath, p.ConsistencyPath, p.XXX_NoUnkeyedLiteral, p.XXX_unrecognized, p.XXX_sizecache}

			root := new(schema.Root)
			rootc, _ := r.rs.GetRoot(ctx)
			root.Root = rootc.Root
			root.Index = rootc.Index

			w.Header().Set("Content-Type", "application/json")
			buf, err := marshaler.Marshal(resp)
			if err != nil {
				panic(err)
			}
			var m map[string]interface{}
			err = json.Unmarshal(buf, &m)
			if err != nil {
				panic(err)
			}
			/* remember to calc the leaf hash from key val with values that are coming from client and index from server.
			DO NOT USE leaf generated from server for security reasons. (maybe somebody can create a temper leaf)
			*/
			verified := proof.Verify(p.Leaf, *root)
			m["verified"] = verified
			newData, _ := json.Marshal(m)
			if verified {
				//saving a fresh root
				tocache := new(schema.Root)
				tocache.Index = p.Index
				tocache.Root = p.Root
				err := r.rs.SetRoot(tocache)
				if err != nil {
					panic(err)
				}
			}
			w.Write(newData)
			return
		}
	}
}
