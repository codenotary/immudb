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

	"github.com/codenotary/immudb/pkg/client"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type HistoryHandler interface {
	History(w http.ResponseWriter, req *http.Request, pathParams map[string]string)
}

type historyHandler struct {
	mux    *runtime.ServeMux
	client client.ImmuClient
}

func NewHistoryHandler(mux *runtime.ServeMux, client client.ImmuClient) HistoryHandler {
	return &historyHandler{
		mux,
		client,
	}
}

func (h *historyHandler) History(w http.ResponseWriter, req *http.Request, pathParams map[string]string) {
	ctx, cancel := context.WithCancel(req.Context())
	defer cancel()
	_, outboundMarshaler := runtime.MarshalerForRequest(h.mux, req)
	rctx, err := runtime.AnnotateContext(ctx, h.mux, req)
	if err != nil {
		runtime.HTTPError(ctx, h.mux, outboundMarshaler, w, req, err)
		return
	}
	var metadata runtime.ServerMetadata

	var (
		val string
		ok  bool
	)

	val, ok = pathParams["key"]
	if !ok {
		runtime.HTTPError(ctx, h.mux, outboundMarshaler, w, req, status.Errorf(codes.InvalidArgument, "missing parameter %s", "key"))
		return
	}

	key, err := runtime.Bytes(val)

	if err != nil {
		runtime.HTTPError(ctx, h.mux, outboundMarshaler, w, req, status.Errorf(codes.InvalidArgument, "type mismatch, parameter: %s, error: %v", "key", err))
		return
	}

	msg, err := h.client.History(rctx, key)
	if err != nil {
		runtime.HTTPError(ctx, h.mux, outboundMarshaler, w, req, err)
		return
	}

	ctx = runtime.NewServerMetadataContext(ctx, metadata)
	w.Header().Set("Content-Type", "application/json")
	newData, err := json.Marshal(msg)
	if err != nil {
		runtime.HTTPError(ctx, h.mux, outboundMarshaler, w, req, err)
		return
	}
	if _, err := w.Write(newData); err != nil {
		runtime.HTTPError(ctx, h.mux, outboundMarshaler, w, req, err)
		return
	}
}
