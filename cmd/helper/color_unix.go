// +build linux darwin freebsd

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

package helper

import (
	"fmt"
	"io"
)

// Reset resets the color
var Reset = "\033[0m"

// Red ...
var Red = "\033[31m"

// Green ...
var Green = "\033[32m"

// Yellow ...
var Yellow = "\033[33m"

// Blue ...
var Blue = "\033[34m"

// Purple ...
var Purple = "\033[35m"

// Cyan ...
var Cyan = "\033[36m"

// White ...
var White = "\033[37m"

// PrintfColorW ...
func PrintfColorW(w io.Writer, color string, format string, args ...interface{}) {
	fmt.Fprintf(w, color+format+Reset, args...)
}
