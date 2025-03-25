/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"crypto/tls"

	pserr "github.com/codenotary/immudb/pkg/pgsql/errors"
)

func (s *session) handshake() error {
	if s.tlsConfig == nil || len(s.tlsConfig.Certificates) == 0 {
		return pserr.ErrSSLNotSupported
	}
	tlsConn := tls.Server(s.mr.Connection(), s.tlsConfig)
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	s.mr.UpgradeConnection(tlsConn)
	return nil
}
