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
	"archive/zip"
	"compress/flate"
	"io"
	"os"
	"strings"

	"github.com/codenotary/immudb/pkg/immuos"
)

// Zip compression levels
const (
	ZipNoCompression      = flate.NoCompression
	ZipBestSpeed          = flate.BestSpeed
	ZipMediumCompression  = 5
	ZipBestCompression    = flate.BestCompression
	ZipDefaultCompression = flate.DefaultCompression
	ZipHuffmanOnly        = flate.HuffmanOnly
)

// Ziper ...
type Ziper interface {
	ZipIt(src, dst string, compressionLevel int) error
	UnZipIt(src, dst string) error
}

// StandardZiper ...
type StandardZiper struct {
	OS       immuos.OS
	ZipItF   func(src, dst string, compressionLevel int) error
	UnZipItF func(src, dst string) error
}

// NewStandardZiper ...
func NewStandardZiper() *StandardZiper {
	sz := &StandardZiper{
		OS: immuos.NewStandardOS(),
	}
	sz.ZipItF = sz.zipIt
	sz.UnZipItF = sz.unZipIt
	return sz
}

// ZipIt ...
func (sz *StandardZiper) ZipIt(src, dst string, compressionLevel int) error {
	return sz.ZipItF(src, dst, compressionLevel)
}

// UnZipIt ...
func (sz *StandardZiper) UnZipIt(src, dst string) error {
	return sz.UnZipItF(src, dst)
}

// zipIt ...
func (sz *StandardZiper) zipIt(src, dst string, compressionLevel int) error {
	srcInfo, err := sz.OS.Stat(src)
	if err != nil {
		return err
	}
	var baseDir string
	if srcInfo.IsDir() {
		baseDir = sz.OS.Base(src)
	}

	if _, err = sz.OS.Stat(dst); err == nil {
		return os.ErrExist
	}
	zipfile, err := sz.OS.Create(dst)
	if err != nil {
		return err
	}
	defer zipfile.Close()

	zipWriter := zip.NewWriter(zipfile)
	defer zipWriter.Close()
	if compressionLevel != ZipDefaultCompression {
		zipWriter.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
			return flate.NewWriter(out, flate.BestCompression)
		})
	}

	err = sz.OS.Walk(src, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		header, err := zip.FileInfoHeader(info)
		if err != nil {
			return err
		}
		if baseDir != "" {
			header.Name = sz.OS.Join(baseDir, strings.TrimPrefix(path, src))
		}
		if info.IsDir() {
			header.Name += "/"
		} else {
			header.Method = zip.Deflate
		}
		writer, err := zipWriter.CreateHeader(header)
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		file, err := sz.OS.Open(path)
		if err != nil {
			return err
		}
		defer file.Close()
		_, err = io.Copy(writer, file)
		return err
	})

	if err != nil {
		return err
	}
	return zipWriter.Close()
}

// unZipIt ...
func (sz *StandardZiper) unZipIt(src, dst string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

	sz.OS.MkdirAll(dst, 0755)

	for _, f := range r.File {
		err := sz.extractAndWriteFile(f, dst)
		if err != nil {
			return err
		}
	}

	return nil
}

func (sz *StandardZiper) extractAndWriteFile(fileInZip *zip.File, dst string) error {
	rc, err := fileInZip.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	path := sz.OS.Join(dst, fileInZip.Name)

	if fileInZip.FileInfo().IsDir() {
		sz.OS.MkdirAll(path, 0755)
	} else {
		if err := sz.OS.MkdirAll(sz.OS.Dir(path), 0755); err != nil {
			return err
		}
		targetFile, err :=
			sz.OS.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileInZip.Mode())
		if err != nil {
			return err
		}
		defer targetFile.Close()

		_, err = io.Copy(targetFile, rc)
		if err != nil {
			return err
		}
	}
	return nil
}
