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
package appendable

type Appendable interface {
	Metadata() []byte
	Size() (int64, error)
	Offset() int64
	SetOffset(off int64) error
	Append(bs []byte) (off int64, n int, err error)
	Flush() error
	Sync() error
	ReadAt(bs []byte, off int64) (int, error)
	Close() error
}
