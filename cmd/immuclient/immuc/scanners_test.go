/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package immuc_test

import (
	"strings"
	"testing"
)

func TestZScan(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	_, err = ic.Imc.ZAdd([]string{"set", "10.5", "key"})
	if err != nil {
		t.Fatal("ZAdd fail", err)
	}

	msg, err := ic.Imc.ZScan([]string{"set"})
	if err != nil {
		t.Fatal("ZScan fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("ZScan failed: %s", msg)
	}
}

func TestIScan(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.VerifiedSet([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}
}

func TestScan(t *testing.T) {
	ic := setupTest(t)

	_, err := ic.Imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	msg, err := ic.Imc.Scan([]string{"k"})
	if err != nil {
		t.Fatal("Scan fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("Scan failed: %s", msg)
	}
}

func TestCount(t *testing.T) {
	t.SkipNow()

	ic := setupTest(t)

	_, err := ic.Imc.Set([]string{"key", "val"})
	if err != nil {
		t.Fatal("Set fail", err)
	}

	msg, err := ic.Imc.Count([]string{"key"})
	if err != nil {
		t.Fatal("Count fail", err)
	}
	if !strings.Contains(msg, "1") {
		t.Fatalf("Count failed: %s", msg)
	}
}
