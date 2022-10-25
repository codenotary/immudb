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

func TestHistory(t *testing.T) {
	ic := setupTest(t)

	msg, err := ic.Imc.History([]string{"key"})
	if err != nil {
		t.Fatal("History fail", err)
	}
	if !strings.Contains(msg, "key not found") {
		t.Fatalf("History fail %s", msg)
	}

	_, err = ic.Imc.Set([]string{"key", "value"})
	if err != nil {
		t.Fatal("History fail", err)
	}
	msg, err = ic.Imc.History([]string{"key"})
	if err != nil {
		t.Fatal("History fail", err)
	}
	if !strings.Contains(msg, "value") {
		t.Fatalf("History fail %s", msg)
	}
}
