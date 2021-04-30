/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http//www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package bmessages

import (
	"bytes"
	"encoding/binary"
)

type errorResp struct {
	fields map[byte]string
}

func ErrorResponse(setters ...Option) []byte {
	messageType := []byte(`E`)
	messageLength := make([]byte, 4)
	// fields
	er := &errorResp{
		fields: make(map[byte]string),
	}
	for _, setter := range setters {
		setter(er)
	}

	body := make([]byte, 0)
	for code, value := range er.fields {
		body = append(body, bytes.Join([][]byte{{code}, []byte(value), {0}}, nil)...)
	}

	binary.BigEndian.PutUint32(messageLength, uint32(len(body)))

	erb := bytes.Join([][]byte{messageType, messageLength, body, {0}}, nil)
	return erb
}
