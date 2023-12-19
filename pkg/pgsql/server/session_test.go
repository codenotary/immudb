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

package server

import (
	"context"

	"github.com/codenotary/immudb/pkg/database"
)

type sessionMock struct {
	InitializeSessionF func() error
	QueryMachineF      func(ctx context.Context) error
	HandleStartupF     func() error
}

func NewSessionMock() *sessionMock {
	s := &sessionMock{
		InitializeSessionF: func() error {
			return nil
		},
		QueryMachineF: func(ctx context.Context) error {
			return nil
		},
		HandleStartupF: func() error {
			return nil
		},
	}
	return s
}

func (s *sessionMock) InitializeSession() error {
	return s.InitializeSessionF()
}

func (s *sessionMock) QueriesMachine(ctx context.Context) error {
	return s.QueryMachineF(ctx)
}

func (s *sessionMock) HandleStartup(dbList database.DatabaseList) error {
	return s.HandleStartupF()
}

func (s *sessionMock) ErrorHandle(e error) {}
