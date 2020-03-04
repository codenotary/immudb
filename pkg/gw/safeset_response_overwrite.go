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
	"net/http"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"

	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

type SafeSetResponseOverwrite interface {
	call(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) error
}

type safeSetResponseOverwrite struct {
	rs client.RootService
}

func NewSafeSetResponseOverwrite(rs client.RootService) SafeSetResponseOverwrite {
	return safeSetResponseOverwrite{rs}
}

func (r safeSetResponseOverwrite) call(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) error {
	if p, ok := resp.(*schema.Proof); ok {
		root, err := r.rs.GetRoot(ctx)
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/json")
		m := make(map[string]bool)
		// The server-generated leaf SHOULD NOT BE USED for security reasons,
		// maybe somebody can create a temper leaf.
		// In this case, we rely on SafeSetRequestOverwrite.call that has validated it
		// already, so p.Leaf and the item's hash are guaranteed to be equal.
		verified := p.Verify(p.Leaf, *root)
		m["verified"] = verified
		newData, _ := json.Marshal(m)
		if verified {
			//saving a fresh root
			tocache := new(schema.Root)
			tocache.Index = p.Index
			tocache.Root = p.Root
			err := r.rs.SetRoot(tocache)
			if err != nil {
				return err
			}
		}
		if _, err := w.Write(newData); err != nil {
			return err
		}
		return nil
	}
	return nil
}
