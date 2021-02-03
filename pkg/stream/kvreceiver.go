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

package stream

import (
	"bufio"
	"bytes"
)

type kvStreamReceiver struct {
	s MsgReceiver
}

func NewKvStreamReceiver(s MsgReceiver) *kvStreamReceiver {
	return &kvStreamReceiver{
		s: s,
	}
}

func (kvr *kvStreamReceiver) Recv() (*KeyValue, error) {
	kv := &KeyValue{}
	key, err := kvr.s.Recv()
	if err != nil {
		return nil, err
	}
	bk := bytes.NewBuffer(key)
	rk := bufio.NewReader(bk)
	// todo @michele a reader and a len(key) should be returned by kvr.s.Recv()
	kv.Key = &ValueSize{
		Content: rk,
		Size:    len(key),
	}

	val, err := kvr.s.Recv()
	if err != nil {
		return nil, err
	}
	bv := bytes.NewBuffer(val)
	rv := bufio.NewReader(bv)
	// todo @michele a reader and a len(val) should be returned by kvr.s.Recv()
	kv.Value = &ValueSize{
		Content: rv,
		Size:    len(val),
	}
	return kv, nil
}
