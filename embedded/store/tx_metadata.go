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
package store

const maxTxMetadataLen = 0

type TxMetadata struct {
}

func NewTxMetadata() *TxMetadata {
	return &TxMetadata{}
}

func (md *TxMetadata) Equal(amd *TxMetadata) bool {
	return true
}

func (md *TxMetadata) Bytes() []byte {
	return nil
}

func (md *TxMetadata) ReadFrom(b []byte) error {
	if len(b) != 0 {
		return ErrCorruptedData
	}
	return nil
}
