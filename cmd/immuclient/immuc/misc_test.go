/*
Copyright 2019-2020 vChain, Inc.

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

package immuc

import (
	"strings"
	"testing"

	"github.com/codenotary/immudb/cmd/immuclient/immuclienttest"
	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestHealthCheckSucceeds(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc := newClient(&immuclienttest.PasswordReader{
		Pass: []string{},
	}, bs.Dialer)
	imc = login("immudb", "immudb", bs.Dialer)
	msg, err := imc.HealthCheck([]string{})
	if err != nil {
		t.Fatal("HealthCheck fail", err)
	}
	if !strings.Contains(msg, "Health check OK") {
		t.Fatal("HealthCheck fail")
	}
}

func TestHealthCheckFails(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := newClient(&testPasswordReader{
		pass: []string{},
	}, bs.Dialer)
	bs.GrpcServer.Stop()
	msg, err := imc.HealthCheck([]string{})
	if err != nil {
		t.Fatal("HealthCheck fail stoped server", err)
	}
	if !strings.Contains(msg, "Error while dialing closed") {
		t.Fatal("HealthCheck fail stoped server", msg)
	}
}

func TestHistory(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()

	imc := login("immudb", "immudb", bs.Dialer)
	msg, err := imc.History([]string{"key"})
	if err != nil {
		t.Fatal("History fail", err)
	}
	if !strings.Contains(msg, "No item found") {
		t.Fatalf("History fail %s", msg)
	}

	msg, err = imc.Set([]string{"key", "value"})
	if err != nil {
		t.Fatal("History fail", err)
	}
	msg, err = imc.History([]string{"key"})
	if err != nil {
		t.Fatal("History fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("History fail %s", msg)
	}
}
