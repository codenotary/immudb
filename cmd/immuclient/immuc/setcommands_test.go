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

func TestRawSafeSet(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	msg, err := imc.RawSafeSet([]string{"key", "val"})
	if err != nil {
		t.Fatal("RawSafeSet fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("RawSafeSet failed: %s", msg)
	}
}
func TestSet(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	msg, err := imc.Set([]string{"key", "val"})

	if err != nil {
		t.Fatal("Set fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("Set failed: %s", msg)
	}
}
func TestSafeSet(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	msg, err := imc.SafeSet([]string{"key", "val"})

	if err != nil {
		t.Fatal("SafeSet fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("SafeSet failed: %s", msg)
	}
}
func TestZAdd(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	_, _ = imc.SafeSet([]string{"key", "val"})

	msg, err := imc.ZAdd([]string{"val", "1", "key"})

	if err != nil {
		t.Fatal("SZAddafeSet fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("ZAdd failed: %s", msg)
	}
}
func TestSafeZAdd(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	_, _ = imc.SafeSet([]string{"key", "val"})

	msg, err := imc.SafeZAdd([]string{"val", "1", "key"})

	if err != nil {
		t.Fatal("SafeZAdd fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("SafeZAdd failed: %s", msg)
	}
}
func TestCreateDatabase(t *testing.T) {
	options := server.DefaultOptions().WithAuth(true).WithInMemoryStore(true)
	bs := servertest.NewBufconnServer(options)
	bs.Start()
	imc := login("immudb", "immudb", bs.Dialer)

	msg, err := imc.CreateDatabase([]string{"newdb"})
	if err != nil {
		t.Fatal("CreateDatabase fail", err)
	}
	if !strings.Contains(msg, "Created Database") {
		t.Fatalf("CreateDatabase failed: %s", msg)
	}

	msg, err = imc.DatabaseList([]string{})
	if err != nil {
		t.Fatal("DatabaseList fail", err)
	}

	msg, err = imc.UseDatabase([]string{"newdb"})
	if err != nil {
		t.Fatal("UseDatabase fail", err)
	}
	if !strings.Contains(msg, "newdb") {
		t.Fatalf("UseDatabase failed: %s", msg)
	}
}
