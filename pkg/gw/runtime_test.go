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
	"errors"
	"net/http"
	"testing"

	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/stretchr/testify/require"
)

func newTestRuntimeWithAnnotateContextErr() Runtime {
	rt := newDefaultRuntime()
	rt.annotateContext =
		func(ctx context.Context, mux *runtime.ServeMux, req *http.Request) (context.Context, error) {
			return ctx, errors.New("annotate context error")
		}
	return rt
}

func TestRuntime(t *testing.T) {
	rt := DefaultRuntime()
	drt, ok := rt.(*defaultRuntime)
	require.True(t, ok)

	ctx := context.Background()

	annotateContextErr := errors.New("annotate context error")
	drt.annotateContext =
		func(ctx context.Context, mux *runtime.ServeMux, req *http.Request) (context.Context, error) {
			return ctx, annotateContextErr
		}
	_, err := drt.AnnotateContext(ctx, nil, nil)
	require.Equal(t, annotateContextErr, err)

	toBytesErr := errors.New("to bytes error")
	drt.toBytes = func(val string) ([]byte, error) {
		return nil, toBytesErr
	}
	_, err = drt.Bytes("")
	require.Equal(t, toBytesErr, err)

	var httpError string
	drt.httpError =
		func(context.Context, *runtime.ServeMux, runtime.Marshaler, http.ResponseWriter, *http.Request, error) {
			httpError = "some http error"
		}
	drt.HTTPError(ctx, nil, nil, nil, nil, nil)
	require.Equal(t, "some http error", httpError)

	marshalerForRequestCalled := false
	drt.marshalerForRequest =
		func(*runtime.ServeMux, *http.Request) (i runtime.Marshaler, o runtime.Marshaler) {
			marshalerForRequestCalled = true
			return nil, nil
		}
	drt.MarshalerForRequest(nil, nil)
	require.True(t, marshalerForRequestCalled)

	newServerMetadataContextCalled := false
	drt.newServerMetadataContext = func(ctx context.Context, md runtime.ServerMetadata) context.Context {
		newServerMetadataContextCalled = true
		return ctx
	}
	drt.NewServerMetadataContext(ctx, *new(runtime.ServerMetadata))
	require.True(t, newServerMetadataContextCalled)
}
