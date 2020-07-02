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
	"archive/tar"
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// TarIt takes a source and variable writers and walks 'source' writing each file
// found to the tar writer; the purpose for accepting multiple writers is to allow
// for multiple outputs (for example a file, or md5 hash)
func TarIt(src string, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	var baseDir string
	if info.IsDir() {
		baseDir = filepath.Base(src)
	}

	if _, err = os.Stat(dst); err == nil {
		return os.ErrExist
	}
	destFile, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer destFile.Close()

	mw := io.MultiWriter(destFile)
	gzw := gzip.NewWriter(mw)
	defer gzw.Close()
	tw := tar.NewWriter(gzw)
	defer tw.Close()

	return filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
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
			header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, src))
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
		f, err := os.Open(path)
		if err != nil {
			return err
		}
		defer f.Close()
		_, err = io.Copy(tw, f)
		return err
	})
}

// UnTarIt takes a destination path and a reader; a tar reader loops over the tarfile
// creating the file structure at 'dst' along the way, and writing any files
func UnTarIt(src string, dst string) error {
	srcFile, err := os.Open(src)
	if err != nil {
		return err
	}
	if err = os.MkdirAll(dst, 0755); err != nil {
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
		if err = unTarEntry(dst, tr, header); err != nil {
			return err
		}
	}
}

func unTarEntry(dst string, tr io.Reader, header *tar.Header) error {
	if header == nil {
		return nil
	}
	target := filepath.Join(dst, header.Name)
	switch header.Typeflag { // or header.FileInfo() - same thing
	case tar.TypeDir:
		if err := os.MkdirAll(target, 0755); err != nil {
			return err
		}
	case tar.TypeReg:
		dir, _ := filepath.Split(target)
		if err := os.MkdirAll(dir, 0755); err != nil {
			return err
		}
		if err := copyFile(target, tr, header.FileInfo().Mode()); err != nil {
			return err
		}
	}
	return nil
}

func copyFile(dst string, src io.Reader, perm os.FileMode) error {
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, src)
	return err
}
