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

package bmessages

import (
	"bytes"
	"encoding/binary"
)

func CommandComplete() []byte {
	messageType := []byte(`C`)
	message := []byte(`SELECT 1`)
	message = bytes.Join([][]byte{message, {0}}, nil)
	selfMessageLength := make([]byte, 4)
	binary.BigEndian.PutUint32(selfMessageLength, uint32(len(message)+4))

	return bytes.Join([][]byte{messageType, selfMessageLength, message}, nil)
}
