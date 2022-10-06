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
	"log"
	"strings"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
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
	for i := 0; i < 5; i++ {
		log.Printf("Error %v Invoking Retry method %s \n", err, method)
		time.Sleep(time.Millisecond * 200)
		if strings.Contains(err.Error(), "session not found") {
			log.Println("Wait until ReOpen Session")

			if err := c.sessionRenewal(context.Background()); err != nil {
				log.Println("UNABLE TO ReOpen Session")
				log.Printf("error is %v \n", err)
			}

			cc.Connect()
			currentState := cc.GetState()
			stillConnecting := true

			for currentState != connectivity.Ready && stillConnecting {
				log.Printf("Attempting reconnection., state is %s \n", currentState)
				//will return true when state has changed from thisState, false if timeout
				cx, cancel := context.WithTimeout(context.Background(), 10*time.Second) //define how long you want to wait for connection to be restored before giving up
				stillConnecting = cc.WaitForStateChange(cx, currentState)
				cancel()
				currentState = cc.GetState()
				log.Printf("Attempting reconnection., state is %s \n", currentState)
			}

			err = invoker(ctx, method, req, reply, cc, opts...)
			if err == nil {
				return nil
			}
		}
	}
	return err
}
