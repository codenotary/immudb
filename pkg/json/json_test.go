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

package json

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestJSON(t *testing.T) {
	sj := DefaultJSON()

	marshalErr := errors.New("marshal error")
	sj.MarshalF =
		func(v interface{}) ([]byte, error) {
			return nil, marshalErr
		}
	_, err := sj.Marshal(nil)
	require.Equal(t, marshalErr, err)

	unmarshalErr := errors.New("unmarshal error")
	sj.UnmarshalF =
		func(data []byte, v interface{}) error {
			return unmarshalErr
		}
	err = sj.Unmarshal(nil, nil)
	require.Equal(t, unmarshalErr, err)
}
