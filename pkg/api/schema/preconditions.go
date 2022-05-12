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
package schema

func PreconditionKeyMustExist(key []byte) *Precondition {
	return &Precondition{
		Precondition: &Precondition_KeyMustExist{
			KeyMustExist: &Precondition_KeyMustExistPrecondition{
				Key: key,
			},
		},
	}
}

func PreconditionKeyMustNotExist(key []byte) *Precondition {
	return &Precondition{
		Precondition: &Precondition_KeyMustNotExist{
			KeyMustNotExist: &Precondition_KeyMustNotExistPrecondition{
				Key: key,
			},
		},
	}
}

func PreconditionKeyNotModifiedAfterTX(key []byte, txID uint64) *Precondition {
	return &Precondition{
		Precondition: &Precondition_KeyNotModifiedAfterTX{
			KeyNotModifiedAfterTX: &Precondition_KeyNotModifiedAfterTXPrecondition{
				Key:  key,
				TxID: txID,
			},
		},
	}
}
