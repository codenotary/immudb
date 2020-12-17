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

package database

const (
	setKeyPrefix = iota
	sortedSetKeyPrefix
)

const (
	plainValuePrefix = iota
	referenceValuePrefix
)

//WrapWithPrefix ...
func wrapWithPrefix(b []byte, prefix byte) []byte {
	wb := make([]byte, 1+len(b))
	wb[0] = prefix
	copy(wb[1:], b)
	return wb
}

//unWrapWithPrefix ...
func unWrapWithPrefix(wb []byte) (b []byte, prefix byte) {
	b = make([]byte, len(wb)-1)
	prefix = wb[0]
	return
}
