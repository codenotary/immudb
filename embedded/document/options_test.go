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
package document

import (
	"testing"

	"github.com/codenotary/immudb/embedded/store"
	"github.com/stretchr/testify/require"
)

func TestDefaultOptions(t *testing.T) {
	// Call the DefaultOptions function
	opts := DefaultOptions()

	// Assert that the returned value is not nil
	require.NotNil(t, opts)

	require.Equal(t, DefaultDocumentMaxNestedFields, opts.maxNestedFields)
}

func TestOptionsValidate(t *testing.T) {
	// Test case with non-nil options
	opts := &Options{}
	err := opts.Validate()
	require.Nil(t, err, "Expected no error for non-nil options")

	// Test case with nil options
	var nilOpts *Options
	err = nilOpts.Validate()
	require.NotNil(t, err, "Expected error for nil options")
	require.ErrorIs(t, err, store.ErrInvalidOptions, "Expected ErrInvalidOptions error")
	require.Equal(t, "illegal arguments: invalid options: nil options", err.Error(), "Expected specific error message")
}

func TestOptionsWithPrefix(t *testing.T) {
	// Create initial options
	opts := &Options{}

	// Call the WithPrefix method
	prefix := []byte("test")
	newOpts := opts.WithPrefix(prefix)

	// Assert that the returned options have the correct prefix
	require.Equal(t, prefix, newOpts.prefix, "Expected prefix to be set in the new options")
}

func TestOptionsWithMaxNestedFields(t *testing.T) {
	opts := DefaultOptions().WithMaxNestedFields(20)

	require.Equal(t, 20, opts.maxNestedFields)
}
