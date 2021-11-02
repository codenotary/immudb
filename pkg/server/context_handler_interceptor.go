/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package server

import (
	"context"
	"github.com/codenotary/immudb/pkg/errors"
	"google.golang.org/grpc"
)

// ContextHandlerInterceptor
func ContextHandlerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	var err error
	var result interface{}

	done := make(chan struct{})

	go func() {
		result, err = handler(ctx, req)
		done <- struct{}{}
	}()
	if e := checkCtxError(ctx, done); e != nil {
		return nil, e
	}
	return result, err
}

// StreamContextHandlerInterceptor
func StreamContextHandlerInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	ctx := ss.Context()
	var err error

	done := make(chan struct{})

	go func() {
		err = handler(srv, ss)
		done <- struct{}{}
	}()
	if e := checkCtxError(ctx, done); e != nil {
		return e
	}
	return err
}

func checkCtxError(ctx context.Context, done chan struct{}) error {
	select {
	case <-ctx.Done():
		e := ctx.Err()
		switch e {
		case context.Canceled:
			return errors.New(ErrCanceled)
		case context.DeadlineExceeded:
			return errors.New(ErrDeadlineExceeded)
		default:
			return e
		}
	case <-done:
	}
	return nil
}
