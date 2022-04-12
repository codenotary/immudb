/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package heartbeater

import (
	"context"
	stdos "os"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
)

type heartBeater struct {
	sessionID     string
	logger        logger.Logger
	serviceClient schema.ImmuServiceClient
	done          chan bool
	t             *time.Ticker
}

type HeartBeater interface {
	KeepAlive(ctx context.Context)
	Stop()
}

func NewHeartBeater(sessionID string, sc schema.ImmuServiceClient, keepAliveInterval time.Duration) *heartBeater {
	return &heartBeater{
		sessionID:     sessionID,
		logger:        logger.NewSimpleLogger("immuclient", stdos.Stdout),
		serviceClient: sc,
		done:          make(chan bool),
		t:             time.NewTicker(keepAliveInterval),
	}
}

func (hb *heartBeater) KeepAlive(ctx context.Context) {
	go func() {
		for {
			select {
			case <-hb.done:
				return
			case t := <-hb.t.C:
				hb.logger.Debugf("keep alive for %s at %s\n", hb.sessionID, t.String())

				err := hb.keepAliveRequest(ctx)
				if err != nil {
					hb.logger.Errorf("an error occurred on keep alive %s at %s: %v\n", hb.sessionID, t.String(), err)
				}
			}
		}
	}()
}

func (hb *heartBeater) Stop() {
	hb.done <- true
}

func (hb *heartBeater) keepAliveRequest(ctx context.Context) error {
	c, cancel := context.WithTimeout(ctx, time.Second*3)
	defer cancel()

	_, err := hb.serviceClient.KeepAlive(c, new(empty.Empty))
	if err != nil {
		return err
	}

	return err
}
