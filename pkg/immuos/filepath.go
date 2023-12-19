/*
Copyright 2024 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

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
	Walk(root string, walkFn filepath.WalkFunc) error
	FromSlash(path string) string
	Join(elem ...string) string
	Clean(path string) string
	Split(path string) (dir, file string)
}

// StandardFilepath ...
type StandardFilepath struct {
	AbsF       func(path string) (string, error)
	BaseF      func(path string) string
	ExtF       func(path string) string
	DirF       func(path string) string
	WalkF      func(root string, walkFn filepath.WalkFunc) error
	FromSlashF func(path string) string
	JoinF      func(elem ...string) string
	CleanF     func(path string) string
	SplitF     func(path string) (dir, file string)
}

// NewStandardFilepath ...
func NewStandardFilepath() *StandardFilepath {
	return &StandardFilepath{
		AbsF:       filepath.Abs,
		BaseF:      filepath.Base,
		ExtF:       filepath.Ext,
		DirF:       filepath.Dir,
		WalkF:      filepath.Walk,
		FromSlashF: filepath.FromSlash,
		JoinF:      filepath.Join,
		CleanF:     filepath.Clean,
		SplitF:     filepath.Split,
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

// Walk ...
func (sf *StandardFilepath) Walk(root string, walkFn filepath.WalkFunc) error {
	return sf.WalkF(root, walkFn)
}

// FromSlash ...
func (sf *StandardFilepath) FromSlash(path string) string {
	return sf.FromSlashF(path)
}

// Join ...
func (sf *StandardFilepath) Join(elem ...string) string {
	return sf.JoinF(elem...)
}

// Clean ...
func (sf *StandardFilepath) Clean(path string) string {
	return sf.CleanF(path)
}

// Split ...
func (sf *StandardFilepath) Split(path string) (dir, file string) {
	return sf.SplitF(path)
}
