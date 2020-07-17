/*
Copyright 2019-2020 vChain, Inc.

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

package immuos

import "path/filepath"

// Filepath ...
type Filepath interface {
	Abs(path string) (string, error)
	Base(path string) string
	Ext(path string) string
	Dir(path string) string
}

// StandardFilepath ...
type StandardFilepath struct {
	AbsF  func(path string) (string, error)
	BaseF func(path string) string
	ExtF  func(path string) string
	DirF  func(path string) string
}

// NewStandardFilepath ...
func NewStandardFilepath() *StandardFilepath {
	return &StandardFilepath{
		AbsF:  filepath.Abs,
		BaseF: filepath.Base,
		ExtF:  filepath.Ext,
		DirF:  filepath.Dir,
	}
}

// Abs ...
func (sf *StandardFilepath) Abs(path string) (string, error) {
	return sf.AbsF(path)
}

// Base ...
func (sf *StandardFilepath) Base(path string) string {
	return sf.BaseF(path)
}

// Ext ...
func (sf *StandardFilepath) Ext(path string) string {
	return sf.ExtF(path)
}

// Dir ...
func (sf *StandardFilepath) Dir(path string) string {
	return sf.DirF(path)
}
