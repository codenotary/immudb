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
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"net/http"
)

type HistoryHandler interface {
	History(w http.ResponseWriter, req *http.Request, pathParams map[string]string)
}

type historyHandler struct {
	mux    *runtime.ServeMux
	client schema.ImmuServiceClient
	rs     client.RootService
}

func NewHistoryHandler(mux *runtime.ServeMux, client schema.ImmuServiceClient, rs client.RootService) HistoryHandler {
	return &historyHandler{
		mux,
		client,
		rs,
	}
}

func (h *historyHandler) History(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	inboundMarshaler, outboundMarshaler := runtime.MarshalerForRequest(h.mux, req)
	rctx, err := runtime.AnnotateContext(ctx, h.mux, req)
	if err != nil {
		runtime.HTTPError(ctx, h.mux, outboundMarshaler, w, req, err)
		return
	}
	historyRequestOverwrite := NewHistoryRequestOverwrite(h.rs)
	resp, md, err := historyRequestOverwrite.call(rctx, inboundMarshaler, h.client, req, pathParams)

	ctx = runtime.NewServerMetadataContext(ctx, md)
	if err != nil {
		runtime.HTTPError(ctx, h.mux, outboundMarshaler, w, req, err)
		return
	}
	historyResponseOverwrite := NewHistoryResponseOverwrite(h.rs)
	if err := historyResponseOverwrite.call(ctx, h.mux, outboundMarshaler, w, req, resp, h.mux.GetForwardResponseOptions()...); err != nil {
		runtime.HTTPError(ctx, h.mux, outboundMarshaler, w, req, err)
	}

}
