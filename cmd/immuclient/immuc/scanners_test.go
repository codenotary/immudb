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

	"github.com/codenotary/immudb/pkg/server"
	"github.com/codenotary/immudb/pkg/server/servertest"
)

func TestZScan(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)
	_, err := imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	msg, err := imc.ZScan([]string{"key"})
	if err != nil {
		t.Fatal("ZScan fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("ZScan failed: %s", msg)
	}
}

func TestIScan(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)
	_, err := imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	msg, err := imc.IScan([]string{"0", "1"})

	if err != nil {
		t.Fatal("IScan fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("IScan failed: %s", msg)
	}
}

func TestScan(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)
	_, err := imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	msg, err := imc.Scan([]string{"k"})
	if err != nil {
		t.Fatal("Scan fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("Scan failed: %s", msg)
	}
}

func TestCount(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)
	_, err := imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	msg, err := imc.Count([]string{"key"})
	if err != nil {
		t.Fatal("Count fail", err)
	}
	if !strings.Contains(msg, "1") {
		t.Fatalf("Count failed: %s", msg)
	}
}
