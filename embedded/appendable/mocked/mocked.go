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
package mocked

type MockedAppendable struct {
	MetadataFn  func() []byte
	SizeFn      func() (int64, error)
	OffsetFn    func() int64
	SetOffsetFn func(off int64) error
	AppendFn    func(bs []byte) (off int64, n int, err error)
	FlushFn     func() error
	SyncFn      func() error
	ReadAtFn    func(bs []byte, off int64) (int, error)
	CloseFn     func() error
}

func (a *MockedAppendable) Metadata() []byte {
	return a.MetadataFn()
}

func (a *MockedAppendable) Size() (int64, error) {
	return a.SizeFn()
}

func (a *MockedAppendable) Offset() int64 {
	return a.OffsetFn()
}

func (a *MockedAppendable) SetOffset(off int64) error {
	return a.SetOffsetFn(off)
}

func (a *MockedAppendable) Append(bs []byte) (off int64, n int, err error) {
	return a.AppendFn(bs)
}

func (a *MockedAppendable) Flush() error {
	return a.FlushFn()
}

func (a *MockedAppendable) Sync() error {
	return a.SyncFn()
}

func (a *MockedAppendable) ReadAt(bs []byte, off int64) (int, error) {
	return a.ReadAtFn(bs, off)
}

func (a *MockedAppendable) Close() error {
	return a.CloseFn()
}
