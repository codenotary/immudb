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

package clienttest

import (
	"github.com/codenotary/immudb/pkg/client/homedir"
	"github.com/codenotary/immudb/pkg/client/tokenservice"
)

var (
	_ tokenservice.TokenService = (*TokenServiceMock)(nil)
)

type TokenServiceMock struct {
	tokenservice.TokenService
	GetTokenF       func() (string, error)
	SetTokenF       func(database string, token string) error
	IsTokenPresentF func() (bool, error)
	DeleteTokenF    func() error
}

func (ts TokenServiceMock) GetToken() (string, error) {
	return ts.GetTokenF()
}

func (ts TokenServiceMock) SetToken(database string, token string) error {
	return ts.SetTokenF(database, token)
}

func (ts TokenServiceMock) DeleteToken() error {
	return ts.DeleteTokenF()
}

func (ts TokenServiceMock) IsTokenPresent() (bool, error) {
	return ts.IsTokenPresentF()
}

func (ts TokenServiceMock) GetDatabase() (string, error) {
	return "", nil
}

func (ts TokenServiceMock) WithHds(hds homedir.HomedirService) tokenservice.TokenService {
	return ts
}

func (ts TokenServiceMock) WithTokenFileName(tfn string) tokenservice.TokenService {
	return ts
}

// DefaultHomedirServiceMock ...
func DefaultTokenServiceMock() *TokenServiceMock {
	return &TokenServiceMock{
		GetTokenF: func() (string, error) {
			return "", nil
		},
		SetTokenF: func(database string, token string) error {
			return nil
		},
		IsTokenPresentF: func() (bool, error) {
			return true, nil
		},
		DeleteTokenF: func() error {
			return nil
		},
	}
}
