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
	stdos "os"
	"time"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/golang/protobuf/ptypes/empty"
)

type heartBeater struct {
	sessionID     string
	logger        logger.Logger
	serviceClient schema.ImmuServiceClient
	done          chan struct{}
	t             *time.Ticker
	errorHandler  ErrorHandler
}

type HeartBeater interface {
	KeepAlive(ctx context.Context)
	Stop()
}

func NewHeartBeater(sessionID string, sc schema.ImmuServiceClient, keepAliveInterval time.Duration, errhandler ErrorHandler, l logger.Logger) *heartBeater {
	if l == nil {
		l = logger.NewSimpleLogger("immuclient", stdos.Stdout)
	}

	return &heartBeater{
		sessionID:     sessionID,
		logger:        l,
		serviceClient: sc,
		done:          make(chan struct{}),
		t:             time.NewTicker(keepAliveInterval),
		errorHandler:  errhandler,
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
					if hb.errorHandler != nil {
						hb.errorHandler(hb.sessionID, err)
					}
				}
			}
		}
	}()
}

func (hb *heartBeater) Stop() {
	hb.t.Stop()
	close(hb.done)
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
