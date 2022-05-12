// +build windows

/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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

package helper

import (
	"strings"

	"golang.org/x/sys/windows"
)

// ResolvePath
func ResolvePath(path string, quote bool) (finalPath string, err error) {
	var toReplace string
	var folderId *windows.KNOWNFOLDERID
	var token string
	if strings.Contains(path, "%programdata%") {
		toReplace = "%programdata%"
		folderId = windows.FOLDERID_ProgramData
	}
	if strings.Contains(path, "%programfile%") {
		toReplace = "%programfile%"
		folderId = windows.FOLDERID_ProgramFiles
	}
	if toReplace != "" {
		if token, err = windows.KnownFolderPath(folderId, windows.KF_FLAG_DEFAULT); err != nil {
			return "", err
		}
		if quote {
			token = strings.Replace(token, "\\", "\\\\", -1)
		}
		path = strings.Replace(path, toReplace, token, -1)
	}
	return path, nil
}
