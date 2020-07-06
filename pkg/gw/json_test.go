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

package gw

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func newTestJSONWithMarshalErr() JSON {
	j := newDefaultJSON()
	j.marshal = func(v interface{}) ([]byte, error) {
		return nil, errors.New("JSON marshal error")
	}
	return j
}

func TestJSON(t *testing.T) {
	j := DefaultJSON()
	dj, ok := j.(*defaultJSON)
	require.True(t, ok)

	marshalErr := errors.New("marshal error")
	dj.marshal =
		func(v interface{}) ([]byte, error) {
			return nil, marshalErr
		}
	_, err := dj.Marshal(nil)
	require.Equal(t, marshalErr, err)

	unmarshalErr := errors.New("unmarshal error")
	dj.unmarshal =
		func(data []byte, v interface{}) error {
			return unmarshalErr
		}
	err = dj.Unmarshal(nil, nil)
	require.Equal(t, unmarshalErr, err)
}
