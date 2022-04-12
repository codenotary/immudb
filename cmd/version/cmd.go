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

package version

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
)

// App application name
var App string

// Version holds the version
var Version string

// Commit the most recent commit from which this version has been built
var Commit string

// BuiltBy built by email
var BuiltBy string

// BuiltAt date and time of the build
var BuiltAt string

// Static flags the binary as statically linked
var Static string

// VersionCmd returns a new version command
func VersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: fmt.Sprintf("Show the %s version", App),
		Run: func(_ *cobra.Command, _ []string) {
			versionStr := VersionStr()
			if versionStr != "" {
				fmt.Println(versionStr)
			}
		},
	}
}

// VersionStr formats and returns the version string
func VersionStr() string {
	if App == "" || Version == "" {
		return "no version info available"
	}
	pieces := []string{
		fmt.Sprintf("%s %s", App, Version),
	}
	const strPattern = "%-*s: %s"
	const longestLabelLength = 8
	if Commit != "" {
		pieces = append(
			pieces,
			fmt.Sprintf(strPattern, longestLabelLength, "Commit", Commit))
	}
	if BuiltBy != "" {
		pieces = append(
			pieces,
			fmt.Sprintf(strPattern, longestLabelLength, "Built by", BuiltBy))
	}
	if BuiltAt != "" {
		i, err := strconv.ParseInt(BuiltAt, 10, 64)
		if err == nil {
			builtAt := time.Unix(i, 0).Format(time.RFC1123)
			pieces = append(
				pieces,
				fmt.Sprintf(strPattern, longestLabelLength, "Built at", builtAt))
		}
	}
	if Static != "" {
		pieces = append(
			pieces,
			fmt.Sprintf("%-*s: %t", longestLabelLength, "Static", StaticBuild()))
	}
	return fmt.Sprint(strings.Join(pieces, "\n"))
}

// StaticBuild set the flag which marks the binary as statically linked
func StaticBuild() bool {
	return Static == "static"
}
