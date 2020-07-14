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

package fs

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// CopyFile copies the contents of the file named src to the file named
// by dst. The file will be created if it does not already exist. If the
// destination file exists, all it's contents will be replaced by the contents
// of the source file. The file mode will be copied from the source and
// the copied data is synced/flushed to stable storage.
func CopyFile(src, dst string) error {
	in, err := os.Open(src)
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

	out, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE, info.Mode())
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

// CopyDir recursively copies a directory tree, attempting to preserve permissions.
// Source directory must exist, destination directory must *not* exist.
// Symlinks are ignored and skipped.
func CopyDir(src string, dst string) error {
	src = filepath.Clean(src)
	dst = filepath.Clean(dst)

	si, err := os.Stat(src)
	if err != nil {
		return err
	}
	if !si.IsDir() {
		return fmt.Errorf("copy from %s: source is not a directory", src)
	}

	switch _, err := os.Stat(dst); {
	case err == nil:
		return fmt.Errorf("copy to %s: %w", dst, os.ErrExist)
	case !os.IsNotExist(err):
		return err
	}

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		dstPath := filepath.Join(dst, path[len(src):])
		switch {
		case info.Mode()&os.ModeSymlink != 0:
			return nil
		case info.IsDir():
			return os.MkdirAll(dstPath, info.Mode())
		default:
			return CopyFile(path, dstPath)
		}
	})
}
