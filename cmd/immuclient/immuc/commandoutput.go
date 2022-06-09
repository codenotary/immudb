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

package immuc

import (
	"fmt"
)

// CommandOutput represents an output from a command
type CommandOutput interface {
	// Render the output as a string
	Plain() string

	// Get only the essential value
	ValueOnly() string

	// Render the output as JSON object to marshal
	Json() interface{}
}

// errorOutput contains error information
type errorOutput struct {
	err string
}

func (o *errorOutput) ValueOnly() string { return o.err }
func (o *errorOutput) Plain() string     { return o.err }
func (o *errorOutput) Json() interface{} { return map[string]string{"error": o.err} }

// resultOutput contains a single value output (operation result)
type resultOutput struct {
	Result  interface{} `json:"result"`
	Warning string      `json:"warning,omitempty"`
}

func (s *resultOutput) Plain() string {
	if s.Warning != "" {
		return fmt.Sprintf("%v\n%s", s.Result, s.Warning)
	}
	return fmt.Sprint(s.Result)
}

func (s *resultOutput) ValueOnly() string {
	return fmt.Sprint(s.Result)
}

func (s *resultOutput) Json() interface{} {
	return s
}
