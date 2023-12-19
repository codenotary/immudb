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

package fs

import (
	"fmt"
	"io"
	"os"

	"github.com/codenotary/immudb/pkg/immuos"
)

// Copier ...
type Copier interface {
	CopyFile(src, dst string) error
	CopyDir(src string, dst string) error
}

// StandardCopier ...
type StandardCopier struct {
	OS        immuos.OS
	CopyFileF func(src, dst string) error
	CopyDirF  func(src string, dst string) error
}

// NewStandardCopier ...
func NewStandardCopier() *StandardCopier {
	sc := &StandardCopier{
		OS: immuos.NewStandardOS(),
	}
	sc.CopyFileF = sc.copyFile
	sc.CopyDirF = sc.copyDir
	return sc
}

// CopyFile ...
func (sc *StandardCopier) CopyFile(src, dst string) error {
	return sc.CopyFileF(src, dst)
}

// CopyDir ...
func (sc *StandardCopier) CopyDir(src string, dst string) error {
	return sc.CopyDirF(src, dst)
}

// copyFile copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file. The file mode will be copied from the source and
// the copied data is synced/flushed to stable storage.
func (sc *StandardCopier) copyFile(src, dst string) error {
	in, err := sc.OS.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()

	info, err := in.Stat()
	if err != nil {
		return err
	}
	if info.IsDir() {
		return fmt.Errorf("copy from %s: source is a directory", src)
	}

	out, err := sc.OS.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, info.Mode())
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, in)
	if err != nil {
		return err
	}

	return out.Sync()
}

// copyDir recursively copies a directory tree, attempting to preserve permissions.
// Source directory must exist, destination directory must *not* exist.
// Symlinks are ignored and skipped.
func (sc *StandardCopier) copyDir(src string, dst string) error {
	src = sc.OS.Clean(src)
	dst = sc.OS.Clean(dst)

	si, err := sc.OS.Stat(src)
	if err != nil {
		return err
	}
	if !si.IsDir() {
		return fmt.Errorf("copy from %s: source is not a directory", src)
	}

	switch _, err := sc.OS.Stat(dst); {
	case err == nil:
		return fmt.Errorf("copy to %s: %w", dst, os.ErrExist)
	case !sc.OS.IsNotExist(err):
		return err
	}

	return sc.OS.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		dstPath := sc.OS.Join(dst, path[len(src):])
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			return nil
		case info.IsDir():
			return sc.OS.MkdirAll(dstPath, info.Mode())
		default:
			return sc.copyFile(path, dstPath)
		}
	})
}
