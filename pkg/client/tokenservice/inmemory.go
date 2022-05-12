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

package tokenservice

import (
	"sync"
)

type inmemoryTokenService struct {
	sync.RWMutex
	token    string
	database string
}

func NewInmemoryTokenService() *inmemoryTokenService {
	return &inmemoryTokenService{}
}

func (m *inmemoryTokenService) SetToken(database string, token string) error {
	m.Lock()
	defer m.Unlock()
	if token == "" {
		return ErrEmptyTokenProvided
	}
	m.token = token
	m.database = database
	return nil
}

func (m *inmemoryTokenService) IsTokenPresent() (bool, error) {
	m.RLock()
	defer m.RUnlock()
	return m.token != "", nil
}

func (m *inmemoryTokenService) DeleteToken() error {
	m.Lock()
	defer m.Unlock()
	m.token = ""
	m.database = ""
	return nil
}

func (m *inmemoryTokenService) GetToken() (string, error) {
	m.RLock()
	defer m.RUnlock()
	return m.token, nil
}

func (m *inmemoryTokenService) GetDatabase() (string, error) {
	m.RLock()
	defer m.RUnlock()
	return m.database, nil
}
