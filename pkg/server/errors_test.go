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

package server

import (
	"errors"
	"fmt"
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	immuerrors "github.com/codenotary/immudb/pkg/errors"
	"github.com/stretchr/testify/assert"
)

func TestMapServerError(t *testing.T) {
	err := mapServerError(store.ErrIllegalState)
	assert.Equal(t, ErrIllegalState, err)

	err = mapServerError(store.ErrIllegalArguments)
	assert.Equal(t, ErrIllegalArguments, err)

	someError := errors.New("some error")
	err = mapServerError(someError)
	assert.Equal(t, someError, err)

	err = mapServerError(fmt.Errorf("%w: test", store.ErrPreconditionFailed))
	assert.Equal(t, immuerrors.CodIntegrityConstraintViolation, err.(immuerrors.Error).Code())
}
