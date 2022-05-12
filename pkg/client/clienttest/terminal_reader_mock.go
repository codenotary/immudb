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

// Package clienttest ...
package clienttest

// TerminalReaderMock ...
type TerminalReaderMock struct {
	Counter             int
	Responses           []string
	ReadFromTerminalYNF func(string) (string, error)
}

// ReadFromTerminalYN ...
func (t *TerminalReaderMock) ReadFromTerminalYN(def string) (selected string, err error) {
	if t.ReadFromTerminalYNF != nil {
		return t.ReadFromTerminalYNF(def)
	}

	if len(t.Responses) < t.Counter {
		panic("not enough responses")
	}
	resp := t.Responses[t.Counter]
	t.Counter++
	return resp, nil
}
