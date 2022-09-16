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
	"testing"
)

func TestAggregateWithNoErrors(t *testing.T) {
	err := NewAggregate([]error{})
	if err != nil || err.Error() != "" {
		t.Errorf("expected nil, got %#v", err)
	}
}

func TestAggregateWithNil(t *testing.T) {
	errs := []error{nil}
	err := NewAggregate(errs)
	if err != nil || err.Error() != "" {
		t.Errorf("expected nil, got %#v", err)
	}
}

func TestAggregateWithNilAndError(t *testing.T) {
	errs := []error{nil, errors.New("foo")}
	err := NewAggregate(errs)
	if err.Error() != "foo" {
		t.Errorf("expected foo, got %#v", err.Error())
	}
}

func TestAggregateMultipleErrors(t *testing.T) {
	errs := []error{nil, errors.New("foo"), errors.New("baz"), errors.New("bar")}
	err := NewAggregate(errs)

	expected := "[foo, baz, bar]"
	if err.Error() != expected {
		t.Errorf("expected %s, got %#v", expected, err.Error())
	}
}

func TestAggregateWithErrorIs(t *testing.T) {
	a, b, c := errors.New("foo"), errors.New("baz"), errors.New("bar")
	errs := []error{a, b}
	err := NewAggregate(errs)

	if v := err.Is(a); !v {
		t.Errorf("expected true, got %#v", v)
	}

	if v := err.Is(c); v {
		t.Errorf("expected false, got %#v", v)
	}
}

func TestAggregateWithErrorAsAggregate(t *testing.T) {
	a, b, c := errors.New("foo"), errors.New("baz"), errors.New("bar")
	err1 := []error{a, b}
	agg1 := NewAggregate(err1)
	err2 := []error{c, agg1}
	agg2 := NewAggregate(err2)

	if v := agg1.Is(a); !v {
		t.Errorf("expected true, got %#v", v)
	}

	if v := agg2.Is(c); !v {
		t.Errorf("expected true, got %#v", v)
	}

	if v := agg2.Is(a); !v {
		t.Errorf("expected true, got %#v", v)
	}

	expected := "[bar, foo, baz]"
	if agg2.Error() != expected {
		t.Errorf("expected %s, got %#v", expected, agg2.Error())
	}

}
