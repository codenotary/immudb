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

const (
	ZipNoCompression      = flate.NoCompression
	ZipBestSpeed          = flate.BestSpeed
	ZipMediumCompression  = 5
	ZipBestCompression    = flate.BestCompression
	ZipDefaultCompression = flate.DefaultCompression
	ZipHuffmanOnly        = flate.HuffmanOnly
)

func ZipIt(src, dst string, compressionLevel int) error {
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

	srcInfo, err := os.Stat(src)
	if err != nil {
		return err
	}
	var baseDir string
	if srcInfo.IsDir() {
		baseDir = filepath.Base(src)
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

func UnZipIt(src, dst string) error {
	reader, err := zip.OpenReader(src)
	if err != nil {
		return err
	}

	_, err = os.Stat(dst)
	if err != nil {
		if os.IsNotExist(err) {
			if err = os.MkdirAll(dst, 0755); err != nil {
				return err
			}
		} else {
			return err
		}
	}

	for _, file := range reader.File {
		path := filepath.Join(dst, file.Name)
		if file.FileInfo().IsDir() {
			if err := os.MkdirAll(path, file.Mode()); err != nil {
				return err
			}
			continue
		}
		fileReader, err := file.Open()
		if err != nil {
			return err
		}
		defer fileReader.Close()
		targetFile, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, file.Mode())
		if err != nil {
			return err
		}
		defer targetFile.Close()
		if _, err := io.Copy(targetFile, fileReader); err != nil {
			return err
		}
		if err := fileReader.Close(); err != nil {
			return err
		}
		if err := targetFile.Close(); err != nil {
			return err
		}
	}

	return nil
}
