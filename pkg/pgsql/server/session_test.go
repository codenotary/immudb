package server

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

import (
	"github.com/codenotary/immudb/pkg/database"
)

type sessionMock struct {
	InitializeSessionF   func() error
	HandleSimpleQueriesF func() error
	HandleStartupF       func() error
}

func NewSessionMock() *sessionMock {
	s := &sessionMock{
		InitializeSessionF: func() error {
			return nil
		},
		HandleSimpleQueriesF: func() error {
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

func (s *sessionMock) HandleSimpleQueries() error {
	return s.HandleSimpleQueriesF()
}

func (s *sessionMock) HandleStartup(dbList database.DatabaseList) error {
	return s.HandleStartupF()
}

func (s *sessionMock) ErrorHandle(e error) {}
