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

package server

import (
	"github.com/codenotary/immudb/pkg/logger"
	"github.com/codenotary/immudb/pkg/pgsql/server/bmessages"
	"github.com/stretchr/testify/require"
	"net"
	"os"
	"sync"
	"testing"
)

func TestSession_HandleSimpleQueriesUnsupportedMessage(t *testing.T) {

	c1, c2 := net.Pipe()
	mr := &messageReader{
		conn: c1,
	}

	s := session{
		mr:    mr,
		Mutex: sync.Mutex{},
		log:   logger.NewSimpleLogger("test", os.Stdout),
	}

	go func() {
		ready4Query := make([]byte, len(bmessages.ReadyForQuery()))
		c2.Read(ready4Query)
		//unsupported message
		c2.Write([]byte("_"))
		unsupported := make([]byte, 500)
		c2.Read(unsupported)
		ready4Query = make([]byte, len(bmessages.ReadyForQuery()))
		c2.Read(ready4Query)
		// Terminate message
		c2.Write([]byte{'X', 0, 0, 0, 0, 0, 0, 0, 4})
	}()

	err := s.HandleSimpleQueries()

	require.NoError(t, err)

}
