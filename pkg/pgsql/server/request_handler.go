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

package server

import (
	"net"
)

func (s *srv) handleRequest(conn net.Conn) (err error) {
	ss := s.SessionFactory.NewSession(conn, s.Logger, s.sysDb, s.tlsConfig)

	// initialize session
	err = ss.InitializeSession()
	if err != nil {
		return err
	}
	// authentication
	err = ss.HandleStartup(s.dbList)
	if err != nil {
		return err
	}
	// https://www.postgresql.org/docs/current/protocol-flow.html#id-1.10.5.7.4
	err = ss.QueriesMachine()
	if err != nil {
		return err
	}

	return nil
}
