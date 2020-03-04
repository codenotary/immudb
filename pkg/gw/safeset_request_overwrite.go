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
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/golang/protobuf/proto"
	"github.com/grpc-ecosystem/grpc-gateway/runtime"
	"github.com/grpc-ecosystem/grpc-gateway/utilities"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	InvalidItemProof = errors.New("proof does not match the given item")
)

type SafeSetRequestOverwrite interface {
	call(ctx context.Context, marshaler runtime.Marshaler, client schema.ImmuServiceClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error)
}

type safeSetRequestOverwrite struct {
	rs client.RootService
}

func NewSafeSetRequestOverwrite(rs client.RootService) SafeSetRequestOverwrite {
	return safeSetRequestOverwrite{rs}
}

func (r safeSetRequestOverwrite) call(ctx context.Context, marshaler runtime.Marshaler, client schema.ImmuServiceClient, req *http.Request, pathParams map[string]string) (proto.Message, runtime.ServerMetadata, error) {
	var protoReq schema.SafeSetOptions
	var metadata runtime.ServerMetadata

	newReader, berr := utilities.IOReaderFactory(req.Body)
	if berr != nil {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", berr)
	}
	if err := marshaler.NewDecoder(newReader()).Decode(&protoReq); err != nil && err != io.EOF {
		return nil, metadata, status.Errorf(codes.InvalidArgument, "%v", err)
	}
	root, err := r.rs.GetRoot(ctx)
	if err != nil {
		return nil, metadata, status.Errorf(codes.Internal, "%v", err)
	}
	ri := new(schema.Index)
	ri.Index = root.Index
	protoReq.RootIndex = ri

	msg, err := client.SafeSet(ctx, &protoReq, grpc.Header(&metadata.HeaderMD), grpc.Trailer(&metadata.TrailerMD))

	// This guard ensures that msg.Leaf is equal to the item's hash
	// computed from request values.
	// From now on, msg.Leaf can be trusted.
	// Thus SafeSetResponseOverwrite will not need to decode the request
	// and compute the hash.
	if err == nil {
		item := schema.Item{
			Key:   protoReq.Kv.Key,
			Value: protoReq.Kv.Value,
			Index: msg.Index,
		}
		if !bytes.Equal(item.Hash(), msg.Leaf) {
			return msg, metadata, InvalidItemProof
		}
	}

	return msg, metadata, err
}
