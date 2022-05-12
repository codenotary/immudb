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
package multierr

import (
	"errors"
	"fmt"
)

type MultiErr struct {
	errors []error
}

func NewMultiErr() *MultiErr {
	return &MultiErr{}
}

func (me *MultiErr) Append(err error) *MultiErr {
	if err != nil {
		me.errors = append(me.errors, err)
	}

	return me
}

func (me *MultiErr) Includes(err error) bool {
	for _, e := range me.errors {
		if errors.Is(e, err) {
			return true
		}
	}

	return false
}

func (me *MultiErr) HasErrors() bool {
	return len(me.errors) > 0
}

func (me *MultiErr) Errors() []error {
	return me.errors
}

func (me *MultiErr) Reduce() error {
	if !me.HasErrors() {
		return nil
	}
	return me
}

func (me *MultiErr) Is(target error) bool {
	for _, err := range me.errors {
		if errors.Is(err, target) {
			return true
		}
	}

	return false
}

func (me *MultiErr) As(target interface{}) bool {
	for _, err := range me.errors {
		if errors.As(err, target) {
			return true
		}
	}

	return false
}

func (me *MultiErr) Error() string {
	return fmt.Sprintf("%v", me.errors)
}
