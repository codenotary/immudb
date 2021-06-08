// Copyright 2014 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package contains a program that generates code to register
// a directory and its contents as zip data for statik file system.
package main

import (
	"archive/zip"
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	spath "path"
	"path/filepath"
	"strings"
	"time"
	"unicode"

	"github.com/rakyll/statik/fs"
)

const nameSourceFile = "statik.go"

var namePackage string

var (
	flagSrc        = flag.String("src", path.Join(".", "public"), "")
	flagDest       = flag.String("dest", ".", "")
	flagNoMtime    = flag.Bool("m", false, "")
	flagNoCompress = flag.Bool("Z", false, "")
	flagForce      = flag.Bool("f", false, "")
	flagTags       = flag.String("tags", "", "")
	flagPkg        = flag.String("p", "statik", "")
	flagNamespace  = flag.String("ns", "default", "")
	flagPkgCmt     = flag.String("c", "", "")
	flagInclude    = flag.String("include", "*.*", "")
)

const helpText = `statik [options]

Options:
-src     The source directory of the assets, "public" by default.
-dest    The destination directory of the generated package, "." by default.

-ns      The namespace where assets will exist, "default" by default.
-f       Override destination if it already exists, false by default.
-include Wildcard to filter files to include, "*.*" by default.
-m       Ignore modification times on files, false by default.
-Z       Do not use compression, false by default.

-p       Name of the generated package, "statik" by default.
-tags    Build tags for the generated package.
-c       Godoc for the generated package.

-help    Prints this text.

Examples:

Generates a statik package from ./assets directory. Overrides
if there is already an existing package.

   $ statik -src=assets -f

Generates a statik package only with the ".js" files
from the ./public directory.

   $ statik -include=*.js
`

// mtimeDate holds the arbitrary mtime that we assign to files when
// flagNoMtime is set.
var mtimeDate = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

func main() {
	flag.Usage = help
	flag.Parse()

	namePackage = *flagPkg

	file, err := generateSource(*flagSrc, *flagInclude)
	if err != nil {
		exitWithError(err)
	}

	destDir := path.Join(*flagDest, namePackage)
	err = os.MkdirAll(destDir, 0755)
	if err != nil {
		exitWithError(err)
	}

	err = rename(file.Name(), path.Join(destDir, nameSourceFile))
	if err != nil {
		exitWithError(err)
	}
}

// rename tries to os.Rename, but fall backs to copying from src
// to dest and unlink the source if os.Rename fails.
func rename(src, dest string) error {
	// Try to rename generated source.
	if err := os.Rename(src, dest); err == nil {
		return nil
	}
	// If the rename failed (might do so due to temporary file residing on a
	// different device), try to copy byte by byte.
	rc, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() {
		rc.Close()
		os.Remove(src) // ignore the error, source is in tmp.
	}()

	if _, err = os.Stat(dest); !os.IsNotExist(err) {
		if *flagForce {
			if err = os.Remove(dest); err != nil {
				return fmt.Errorf("file %q could not be deleted", dest)
			}
		} else {
			return fmt.Errorf("file %q already exists; use -f to overwrite", dest)
		}
	}

	wc, err := os.Create(dest)
	if err != nil {
		return err
	}
	defer wc.Close()

	if _, err = io.Copy(wc, rc); err != nil {
		// Delete remains of failed copy attempt.
		os.Remove(dest)
	}
	return err
}

// Check if an array contains an item
func contains(slice []string, item string) bool {
	set := make(map[string]struct{}, len(slice))
	for _, s := range slice {
		set[s] = struct{}{}
	}

	_, ok := set[item]
	return ok
}

// Match a path with some of inclusions
func match(incs []string, path string) (bool, error) {
	var err error
	for _, inc := range incs {
		matches, e := filepath.Glob(spath.Join(filepath.Dir(path), inc))

		if e != nil {
			err = e
		}

		if matches != nil && len(matches) != 0 && contains(matches, path) {
			return true, nil
		}
	}

	return false, err
}

// Walks on the source path and generates source code
// that contains source directory's contents as zip contents.
// Generates source registers generated zip contents data to
// be read by the statik/fs HTTP file system.
func generateSource(srcPath string, includes string) (file *os.File, err error) {
	var (
		buffer    bytes.Buffer
		zipWriter io.Writer
	)

	zipWriter = &buffer
	f, err := ioutil.TempFile("", namePackage)
	if err != nil {
		return
	}

	zipWriter = io.MultiWriter(zipWriter, f)
	defer f.Close()

	w := zip.NewWriter(zipWriter)
	if err = filepath.Walk(srcPath, func(path string, fi os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Ignore directories and hidden files.
		// No entry is needed for directories in a zip file.
		// Each file is represented with a path, no directory
		// entities are required to build the hierarchy.
		if fi.IsDir() || strings.HasPrefix(fi.Name(), ".") {
			return nil
		}
		relPath, err := filepath.Rel(srcPath, path)
		if err != nil {
			return err
		}
		b, err := ioutil.ReadFile(path)
		if err != nil {
			return err
		}

		incs := strings.Split(includes, ",")

		if b, e := match(incs, path); e != nil {
			return err
		} else if !b {
			return nil
		}

		fHeader, err := zip.FileInfoHeader(fi)
		if err != nil {
			return err
		}
		if *flagNoMtime {
			// Always use the same modification time so that
			// the output is deterministic with respect to the file contents.
			// Do NOT use fHeader.Modified as it only works on go >= 1.10
			fHeader.SetModTime(mtimeDate)
		}
		fHeader.Name = filepath.ToSlash(relPath)
		if !*flagNoCompress {
			fHeader.Method = zip.Deflate
		}
		f, err := w.CreateHeader(fHeader)
		if err != nil {
			return err
		}
		_, err = f.Write(b)
		return err
	}); err != nil {
		return
	}
	if err = w.Close(); err != nil {
		return
	}

	var tags string
	if *flagTags != "" {
		tags = "\n// +build " + *flagTags + "\n"
	}

	var comment string
	if *flagPkgCmt != "" {
		comment = "\n" + commentLines(*flagPkgCmt)
	}

	// e.g.)
	// assetNamespaceIdentify is "AbcDeF_G"
	// when assetNamespace is "abc de f-g"
	assetNamespace := *flagNamespace
	assetNamespaceIdentify := toSymbolSafe(assetNamespace)

	// then embed it as a quoted string
	var qb bytes.Buffer
	fmt.Fprintf(&qb, `// Code generated by statik. DO NOT EDIT.
%s%s
package %s

import (
	"github.com/rakyll/statik/fs"
)

`, tags, comment, namePackage)
	if !fs.IsDefaultNamespace(assetNamespace) {
		fmt.Fprintf(&qb, `
const %s = "%s" // static asset namespace
`, assetNamespaceIdentify, assetNamespace)
	}
	fmt.Fprint(&qb, `
func init() {
	data := "`)
	FprintZipData(&qb, buffer.Bytes())
	if fs.IsDefaultNamespace(assetNamespace) {
		fmt.Fprint(&qb, `"
		fs.Register(data)
	}
	`)

	} else {
		fmt.Fprintf(&qb, `"
		fs.RegisterWithNamespace("%s", data)
	}
	`, assetNamespace)
	}

	if err = ioutil.WriteFile(f.Name(), qb.Bytes(), 0644); err != nil {
		return
	}
	return f, nil
}

// FprintZipData converts zip binary contents to a string literal.
func FprintZipData(dest *bytes.Buffer, zipData []byte) {
	for _, b := range zipData {
		if b == '\n' {
			dest.WriteString(`\n`)
			continue
		}
		if b == '\\' {
			dest.WriteString(`\\`)
			continue
		}
		if b == '"' {
			dest.WriteString(`\"`)
			continue
		}
		if (b >= 32 && b <= 126) || b == '\t' {
			dest.WriteByte(b)
			continue
		}
		fmt.Fprintf(dest, "\\x%02x", b)
	}
}

// comment lines prefixes each line in lines with "// ".
func commentLines(lines string) string {
	lines = "// " + strings.Replace(lines, "\n", "\n// ", -1)
	return lines
}

// Prints out the error message and exists with a non-success signal.
func exitWithError(err error) {
	fmt.Println(err)
	os.Exit(1)
}

// convert src to symbol safe string with upper camel case
func toSymbolSafe(str string) string {
	isBeforeRuneNoGeneralCase := false
	replace := func(r rune) rune {
		if unicode.IsLetter(r) {
			if isBeforeRuneNoGeneralCase {
				isBeforeRuneNoGeneralCase = true
				return r
			} else {
				isBeforeRuneNoGeneralCase = true
				return unicode.ToTitle(r)
			}
		} else if unicode.IsDigit(r) {
			if isBeforeRuneNoGeneralCase {
				isBeforeRuneNoGeneralCase = true
				return r
			} else {
				isBeforeRuneNoGeneralCase = false
				return -1
			}
		} else {
			isBeforeRuneNoGeneralCase = false
			return -1
		}
	}
	return strings.TrimSpace(strings.Map(replace, str))
}

func help() {
	fmt.Println(helpText)
	os.Exit(1)
}
