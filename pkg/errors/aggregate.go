/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package errors

import (
	"errors"
)

var (
	_ error = (*Aggregate)(nil)
)

type Aggregate []error

// NewAggregate encapsulates an array of errors into a single error. It
// satisfies the error interface.
func NewAggregate(errlist []error) Aggregate {
	var filter []error
	for _, err := range errlist {
		if err != nil {
			filter = append(filter, err)
		}
	}
	if len(filter) == 0 {
		return nil
	}

	return Aggregate(filter)
}

// Error returns a concatenated error message, and satisfies the error interface.
func (a Aggregate) Error() (res string) {
	errSet := newSet()
	a.walk(func(err error) bool {
		msg := err.Error()
		if errSet.has(msg) {
			return false
		}
		errSet.add(msg)
		if errSet.len() > 1 {
			res += ", "
		}
		res += msg
		return false
	})
	if errSet.len() == 0 || errSet.len() == 1 {
		return res
	}
	return "[" + res + "]"
}

// Is checks if the target error exists in the aggregate.
func (a Aggregate) Is(target error) bool {
	return a.walk(func(err error) bool {
		return errors.Is(err, target)
	})
}

func (a Aggregate) walk(f func(err error) bool) bool {
	for _, err := range a {
		switch err := err.(type) {
		case Aggregate:
			if match := err.walk(f); match {
				return match
			}
		default:
			if match := f(err); match {
				return match
			}
		}
	}

	return false
}

type set map[string]struct{}

func newSet(items ...string) set {
	ss := make(set, len(items))
	ss.add(items...)
	return ss
}

func (s set) add(items ...string) set {
	for _, item := range items {
		s[item] = struct{}{}
	}
	return s
}

func (s set) has(item string) bool {
	_, ok := s[item]
	return ok
}

func (s set) len() int {
	return len(s)
}
