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
	"testing"
)

func TestZipUnZip(t *testing.T) {
	src := "test-zip-unzip"
	dst := src + ".zip"
	testArchiveUnarchive(
		t,
		src,
		dst,
		func() error { return ZipIt(src, dst, ZipBestSpeed) },
		func() error { return UnZipIt(dst, ".") },
	)
}

func TestZipSrcNonExistent(t *testing.T) {
	nonExistentSrc := "non-existent-zip-src"
	testSrcNonExistent(t, func() error {
		return ZipIt(nonExistentSrc, nonExistentSrc+".zip", ZipBestSpeed)
	})
}

func TestZipDstAlreadyExists(t *testing.T) {
	src := "some-zip-src"
	dst := "existing-zip-dest"
	testDstAlreadyExists(t, src, dst, func() error {
		return ZipIt(src, dst, ZipBestSpeed)
	})
}
