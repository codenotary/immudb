/*
Copyright 2023 Codenotary Inc. All rights reserved.

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
package verification

import (
	"context"
	"testing"

	"github.com/codenotary/immudb/pkg/api/protomodel"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestVerifyDocument(t *testing.T) {
	_, err := VerifyDocument(context.Background(), nil, nil, nil, nil)
	require.ErrorIs(t, err, ErrIllegalArguments)

	t.Run("", func(t *testing.T) {
		doc := &structpb.Struct{
			Fields: map[string]*structpb.Value{
				"pincode": structpb.NewNumberValue(321),
			},
		}

		_, err = VerifyDocument(context.Background(), &protomodel.ProofDocumentResponse{}, doc, nil, nil)
		require.ErrorIs(t, err, ErrIllegalArguments)
	})

}
