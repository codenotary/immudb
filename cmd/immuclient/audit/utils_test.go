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

package audit

import (
	"testing"
)

func TestStringInSlice(t *testing.T) {
	myslice := []string{"app1", "app2", "app3"}
	if !stringInSlice("app1", myslice) {
		t.Fatal("stringInSlice failed, expected true, returned false")
	}
	if stringInSlice("app5", myslice) {
		t.Fatal("stringInSlice failed, expected false, returned true")
	}
}
