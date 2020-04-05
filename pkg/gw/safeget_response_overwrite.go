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

type SafeGetResponseOverwrite interface {
	call(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) error
}

type safeGetResponseOverwrite struct {
	rs client.RootService
}

func NewSafeGetResponseOverwrite(rs client.RootService) SafeGetResponseOverwrite {
	return safeGetResponseOverwrite{rs}
}

func (r safeGetResponseOverwrite) call(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) error {
	if p, ok := resp.(*schema.SafeItem); ok {
		root, err := r.rs.GetRoot(ctx)
		if err != nil {
			return err
		}
		w.Header().Set("Content-Type", "application/json")

		// DO NOT USE leaf generated from server for security reasons
		// (maybe somebody can create a temper leaf)
		si := p.Item.ToSItem()
		verified := p.Proof.Verify(p.Item.Hash(), *root)
		i := &item{p.Item.Key, si.Value.Payload, si.Value.Timestamp, p.Item.Index, verified}

		newData, _ := json.Marshal(i)
		if verified {
			//saving a fresh root
			tocache := new(schema.Root)
			tocache.Index = p.Proof.At
			tocache.Root = p.Proof.Root
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
