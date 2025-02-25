/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"context"
	"sync"

	"github.com/codenotary/immudb/embedded/logger"
	"github.com/codenotary/immudb/embedded/tbtree"
)

const MaxCompactionRequests = 10

type compactionRequest struct {
	job *tbtree.CompactionJob
}

type Compactor struct {
	logger logger.Logger

	mtx    sync.Mutex
	closed bool

	ctx    context.Context
	cancel context.CancelFunc

	reqChan chan compactionRequest
}

func NewCompactor(logger logger.Logger) *Compactor {
	ctx, cancel := context.WithCancel(context.Background())

	return &Compactor{
		logger:  logger,
		ctx:     ctx,
		cancel:  cancel,
		reqChan: make(chan compactionRequest, MaxCompactionRequests),
	}
}

func (c *Compactor) Start() {
	go c.run()
}

func (c *Compactor) SendJob(job *tbtree.CompactionJob) bool {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.closed {
		return false
	}

	select {
	case c.reqChan <- compactionRequest{job: job}:
		return true
	default:
		return false
	}
}

func (c *Compactor) run() {
	for {
		req, ok := <-c.reqChan
		if !ok {
			for req := range c.reqChan {
				req.job.Abort()
			}
			return
		}

		err := req.job.Run(c.ctx)
		if err != nil {
			c.logger.Warningf(
				"%w: unable to compact index at path=%s",
				err,
				req.job.Path(),
			)
		}
	}
}

func (c *Compactor) Close() {
	c.mtx.Lock()
	defer c.mtx.Unlock()

	if c.closed {
		return
	}

	c.cancel()

	close(c.reqChan)
	c.closed = true
}
