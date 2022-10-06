/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package client

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client/cache"
	"github.com/codenotary/immudb/pkg/client/errors"
	"github.com/codenotary/immudb/pkg/client/heartbeater"
	"github.com/codenotary/immudb/pkg/client/state"
	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc"
)

func (c *immuClient) SessionRecoverInterceptor(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	var err error
	err = invoker(ctx, method, req, reply, cc, opts...)
	if err == nil {
		return nil
	}
	if method == "/immudb.schema.ImmuService/CloseSession" || method == "/immudb.schema.ImmuService/OpenSession" {
		log.Printf("Close / Open error %v", err)
		return nil
	}

	if strings.Contains(err.Error(), "session not found") {
		log.Println("Wait until ReOpen Session")

		_ = c.CloseSession(context.Background())
		time.Sleep(time.Second)
		dialOptions := c.SetupDialOptions(c.Options)
		var connErr error
		c.clientConn, connErr = grpc.Dial(c.Options.Bind(), dialOptions...)
		if connErr != nil {
			log.Printf("unable to Dial error %v \n", connErr)
			return connErr
		}
		serviceClient := schema.NewImmuServiceClient(c.clientConn)
		resp, err := serviceClient.OpenSession(ctx, &schema.OpenSessionRequest{
			Username:     c.credentials.user,
			Password:     c.credentials.pass,
			DatabaseName: c.credentials.database,
		})
		if err != nil {
			return errors.FromError(err)
		}
		defer func() {
			if err != nil {
				_, _ = serviceClient.CloseSession(ctx, new(empty.Empty))
			}
		}()

		stateProvider := state.NewStateProvider(serviceClient)

		stateService, err := state.NewStateServiceWithUUID(cache.NewFileCache(c.Options.Dir), c.Logger, stateProvider, resp.GetServerUUID())
		if err != nil {
			return errors.FromError(fmt.Errorf("unable to create state service: %v", err))
		}

		c.ServiceClient = serviceClient
		c.Options.DialOptions = dialOptions
		c.SessionID = resp.GetSessionID()
		c.HeartBeater = heartbeater.NewHeartBeater(c.SessionID, c.ServiceClient, c.Options.HeartBeatFrequency)
		c.HeartBeater.KeepAlive(context.Background())
		c.WithStateService(stateService)
		c.Options.CurrentDatabase = c.credentials.database

		if err := invoker(ctx, method, req, reply, c.clientConn, opts...); err == nil {
			return nil
		}

	}
	return err
}
