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
	"fmt"

	"github.com/codenotary/immudb/embedded/store"
)

const DefaultDocumentMaxNestedFields = 3

type Options struct {
	prefix          []byte
	maxNestedFields int
}

func DefaultOptions() *Options {
	return &Options{
		maxNestedFields: DefaultDocumentMaxNestedFields,
	}
}

func (opts *Options) Validate() error {
	if opts == nil {
		return fmt.Errorf("%w: nil options", store.ErrInvalidOptions)
	}

	return nil
}

func (opts *Options) WithPrefix(prefix []byte) *Options {
	opts.prefix = prefix
	return opts
}

func (opts *Options) WithMaxNestedFields(maxNestedFields int) *Options {
	opts.maxNestedFields = maxNestedFields
	return opts
}
