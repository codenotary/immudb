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

package heartbeater

import (
	"context"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/golang/protobuf/ptypes/empty"
	stdos "os"
	"time"
)

const KeepAliveInterval = time.Second * 1

type heartBeater struct {
	sessionID     string
	logger        logger.Logger
	serviceClient schema.ImmuServiceClient
	done          chan bool
	t             *time.Ticker
}

func NewHeartBeater(sessionID string, sc schema.ImmuServiceClient) *heartBeater {
	return &heartBeater{
		sessionID:     sessionID,
		logger:        logger.NewSimpleLogger("immuclient", stdos.Stdout),
		serviceClient: sc,
		done:          make(chan bool),
		t:             time.NewTicker(KeepAliveInterval),
	}
}

func (hb *heartBeater) KeepAlive() (err error) {
	go func() error {
		for {
			select {
			case <-hb.done:
				return nil
			case t := <-hb.t.C:
				hb.logger.Debugf("keep alive for %d at %s\n", hb.sessionID, t.String())
				err = hb.keepAliveRequest()
				if err != nil {
					return err
				}
			}
		}
	}()
	return err
}

func (hb *heartBeater) keepAliveRequest() error {
	_, err := hb.serviceClient.KeepAlive(context.TODO(), new(empty.Empty))
	if err != nil {
		return err
	}
	return nil
}
