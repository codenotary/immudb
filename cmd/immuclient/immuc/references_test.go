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

func TestReference(t *testing.T) {
	ic := setupTest(t)

	_, _ = ic.Imc.Set([]string{"key", "val"})

	msg, err := ic.Imc.SetReference([]string{"val", "key"})
	if err != nil {
		t.Fatal("Reference fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("Reference failed: %s", msg)
	}
}

func TestVerifiedSetReference(t *testing.T) {
	t.SkipNow()

	ic := setupTest(t)

	_, _ = ic.Imc.Set([]string{"key", "val"})

	msg, err := ic.Imc.VerifiedSetReference([]string{"val", "key"})
	if err != nil {
		t.Fatal("SafeReference fail", err)
	}
	if !strings.Contains(msg, "hash") {
		t.Fatalf("SafeReference failed: %s", msg)
	}
}
