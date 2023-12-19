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
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"strings"

	"github.com/codenotary/immudb/pkg/immuos"
)

// Tarer ...
type Tarer interface {
	TarIt(src string, dst string) error
	UnTarIt(src string, dst string) error
}

// StandardTarer ...
type StandardTarer struct {
	OS       immuos.OS
	TarItF   func(src string, dst string) error
	UnTarItF func(src string, dst string) error
}

// NewStandardTarer ...
func NewStandardTarer() *StandardTarer {
	st := &StandardTarer{
		OS: immuos.NewStandardOS(),
	}
	st.TarItF = st.tarIt
	st.UnTarItF = st.unTarIt
	return st
}

// TarIt ...
func (st *StandardTarer) TarIt(src string, dst string) error {
	return st.TarItF(src, dst)
}

// UnTarIt ...
func (st *StandardTarer) UnTarIt(src string, dst string) error {
	return st.UnTarItF(src, dst)
}

// tarIt takes a source and variable writers and walks 'source' writing each file
// found to the tar writer; the purpose for accepting multiple writers is to allow
// for multiple outputs (for example a file, or md5 hash)
func (st *StandardTarer) tarIt(src string, dst string) error {
	info, err := st.OS.Stat(src)
	if err != nil {
		return err
	}
	var baseDir string
	if info.IsDir() {
		baseDir = st.OS.Base(src)
	}

	if _, err = st.OS.Stat(dst); err == nil {
		return os.ErrExist
	}
	destFile, err := st.OS.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	mw := io.MultiWriter(destFile)
	gzw := gzip.NewWriter(mw)
	defer gzw.Close()
	tw := tar.NewWriter(gzw)
	defer tw.Close()

	return st.OS.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		header, err := tar.FileInfoHeader(info, info.Name())
		if err != nil {
			return err
		}
		if baseDir != "" {
			header.Name = st.OS.Join(baseDir, strings.TrimPrefix(path, src))
		}
		if info.IsDir() {
			header.Name += "/"
		}
		if err = tw.WriteHeader(header); err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		f, err := st.OS.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
}

// unTarIt takes a destination path and a reader; a tar reader loops over the tarfile
// creating the file structure at 'dst' along the way, and writing any files
func (st *StandardTarer) unTarIt(src string, dst string) error {
	srcFile, err := st.OS.Open(src)
	if err != nil {
		return err
	}
	if err = st.OS.MkdirAll(dst, 0755); err != nil {
		return err
	}
	gzr, err := gzip.NewReader(srcFile)
	if err != nil {
		return err
	}
	defer gzr.Close()
	tr := tar.NewReader(gzr)
	for {
		header, err := tr.Next()
		if err != nil {
			if err == io.EOF { // no more files
				return nil
			}
			return err
		}
		if err = st.unTarEntry(dst, tr, header); err != nil {
			return err
		}
	}
}

func (st *StandardTarer) unTarEntry(dst string, tr io.Reader, header *tar.Header) error {
	if header == nil {
		return nil
	}
	target := st.OS.Join(dst, header.Name)
	switch header.Typeflag { // or header.FileInfo() - same thing
	case tar.TypeDir:
		if err := st.OS.MkdirAll(target, 0755); err != nil {
			return err
		}
	case tar.TypeReg:
		dir, _ := st.OS.Split(target)
		if err := st.OS.MkdirAll(dir, 0755); err != nil {
			return err
		}
		if err := st.copyFile(target, tr, header.FileInfo().Mode()); err != nil {
			return err
		}
	}
	return nil
}

func (st *StandardTarer) copyFile(dst string, src io.Reader, perm os.FileMode) error {
	f, err := st.OS.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, src)
	return err
}
