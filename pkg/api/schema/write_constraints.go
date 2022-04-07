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

func WriteConstraintKeyMustExist(key []byte) *WriteConstraint {
	return &WriteConstraint{
		Constraint: &WriteConstraint_KeyMustExist{
			KeyMustExist: &WriteConstraint_KeyMustExistConstraint{
				Key: key,
			},
		},
	}
}

func WriteConstraintKeyMustNotExist(key []byte) *WriteConstraint {
	return &WriteConstraint{
		Constraint: &WriteConstraint_KeyMustNotExist{
			KeyMustNotExist: &WriteConstraint_KeyMustNotExistConstraint{
				Key: key,
			},
		},
	}
}

func WriteConstraintKeyNotModifiedAfterTX(key []byte, txID uint64) *WriteConstraint {
	return &WriteConstraint{
		Constraint: &WriteConstraint_KeyNotModifiedAfterTX{
			KeyNotModifiedAfterTX: &WriteConstraint_KeyNotModifiedAfterTXConstraint{
				Key:  key,
				TxID: txID,
			},
		},
	}
}
