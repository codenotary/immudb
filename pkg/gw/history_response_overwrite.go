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

type HistoryResponseOverwrite interface {
	call(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) error
}

type historyResponseOverwrite struct {
	rs client.RootService
}

func NewHistoryResponseOverwrite(rs client.RootService) HistoryResponseOverwrite {
	return historyResponseOverwrite{rs}
}

func (r historyResponseOverwrite) call(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, req *http.Request, resp proto.Message, opts ...func(context.Context, http.ResponseWriter, proto.Message) error) error {
	if itemList, ok := resp.(*schema.ItemList); ok {
		w.Header().Set("Content-Type", "application/json")
		var items []item
		for _, v := range itemList.Items {
			t := item{
				Key:      v.Key,
				Value:    v.Value,
				Index:    v.Index,
				Verified: false,
			}
			items = append(items, t)
		}
		newData, err := json.Marshal(items)
		if err != nil {
			return err
		}
		if _, err := w.Write(newData); err != nil {
			return err
		}
	}
	return nil
}
