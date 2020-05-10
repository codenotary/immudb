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

package version

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

var App string
var Version string
var Commit string
var BuiltBy string
var BuiltAt string

func VersionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: fmt.Sprintf("Show the %s version", App),
		Run: func(_ *cobra.Command, _ []string) {
			if App == "" || Version == "" {
				return
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
				pieces = append(
					pieces,
					fmt.Sprintf(strPattern, longestLabelLength, "Built at", BuiltAt))
			}
			fmt.Println(strings.Join(pieces, "\n"))
		},
	}
}
