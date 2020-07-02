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
	"archive/zip"
	"compress/flate"
	"io"
	"os"
	"path/filepath"
	"strings"
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

// ZipIt ...
func ZipIt(src, dst string, compressionLevel int) error {
	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	var baseDir string
	if srcInfo.IsDir() {
		baseDir = filepath.Base(src)
	}

	if _, err = os.Stat(dst); err == nil {
		return os.ErrExist
	}
	zipfile, err := os.Create(dst)
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

	err = filepath.Walk(src, func(path string, info os.FileInfo, err error) error {
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
			header.Name = filepath.Join(baseDir, strings.TrimPrefix(path, src))
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
		file, err := os.Open(path)
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

// UnZipIt ...
func UnZipIt(src, dst string) error {
	r, err := zip.OpenReader(src)
	if err != nil {
		return err
	}
	defer r.Close()

	os.MkdirAll(dst, 0755)

	for _, f := range r.File {
		err := extractAndWriteFile(f, dst)
		if err != nil {
			return err
		}
	}

	return nil
}

func extractAndWriteFile(fileInZip *zip.File, dst string) error {
	rc, err := fileInZip.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	path := filepath.Join(dst, fileInZip.Name)

	if fileInZip.FileInfo().IsDir() {
		os.MkdirAll(path, 0755)
	} else {
		os.MkdirAll(filepath.Dir(path), 0755)
		targetFile, err :=
			os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, fileInZip.Mode())
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
