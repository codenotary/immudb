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

package errors

import (
	"context"
	"github.com/codenotary/immudb/pkg/client/errors"
	"google.golang.org/grpc"
)

func ServerStreamInterceptor(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	err := handler(srv, ss)
	if err == nil {
		return nil
	}
	if _, ok := err.(errors.ImmuError); !ok {
		return errors.New(err.Error())
	}
	return err
}

func ServerUnaryInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	i, err := handler(ctx, req)
	if err == nil {
		return i, nil
	}
	if _, ok := err.(errors.ImmuError); !ok {
		return i, errors.New(err.Error())
	}
	return i, err
}
