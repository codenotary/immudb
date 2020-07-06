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
	"net/http"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
)

// Runtime ...
type Runtime interface {
	AnnotateContext(context.Context, *runtime.ServeMux, *http.Request) (context.Context, error)
	Bytes(string) ([]byte, error)
	HTTPError(context.Context, *runtime.ServeMux, runtime.Marshaler, http.ResponseWriter, *http.Request, error)
	MarshalerForRequest(*runtime.ServeMux, *http.Request) (runtime.Marshaler, runtime.Marshaler)
	NewServerMetadataContext(context.Context, runtime.ServerMetadata) context.Context
}

type defaultRuntime struct {
	annotateContext          func(context.Context, *runtime.ServeMux, *http.Request) (context.Context, error)
	toBytes                  func(string) ([]byte, error)
	httpError                func(context.Context, *runtime.ServeMux, runtime.Marshaler, http.ResponseWriter, *http.Request, error)
	marshalerForRequest      func(*runtime.ServeMux, *http.Request) (runtime.Marshaler, runtime.Marshaler)
	newServerMetadataContext func(context.Context, runtime.ServerMetadata) context.Context
}

// DefaultRuntime ...
func DefaultRuntime() Runtime {
	return newDefaultRuntime()
}

func newDefaultRuntime() *defaultRuntime {
	return &defaultRuntime{
		annotateContext:          runtime.AnnotateContext,
		toBytes:                  runtime.Bytes,
		httpError:                runtime.HTTPError,
		marshalerForRequest:      runtime.MarshalerForRequest,
		newServerMetadataContext: runtime.NewServerMetadataContext,
	}
}

func (dr *defaultRuntime) AnnotateContext(ctx context.Context, mux *runtime.ServeMux, req *http.Request) (context.Context, error) {
	return dr.annotateContext(ctx, mux, req)
}
func (dr *defaultRuntime) Bytes(val string) ([]byte, error) {
	return dr.toBytes(val)
}
func (dr *defaultRuntime) HTTPError(ctx context.Context, mux *runtime.ServeMux, marshaler runtime.Marshaler, w http.ResponseWriter, r *http.Request, err error) {
	dr.httpError(ctx, mux, marshaler, w, r, err)
}
func (dr *defaultRuntime) MarshalerForRequest(mux *runtime.ServeMux, r *http.Request) (inbound runtime.Marshaler, outbound runtime.Marshaler) {
	return dr.marshalerForRequest(mux, r)
}
func (dr *defaultRuntime) NewServerMetadataContext(ctx context.Context, md runtime.ServerMetadata) context.Context {
	return dr.newServerMetadataContext(ctx, md)
}
