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

package cmd

import (
	"fmt"
	"strings"

	"github.com/spf13/cobra"
)

func VersionCmd(
	app string,
	version string,
	commit string,
	builtBy string,
	builtAt string,
) *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: fmt.Sprintf("Show the %s version", app),
		Run: func(_ *cobra.Command, _ []string) {
			if app == "" || version == "" {
				return
			}
			pieces := []string{
				fmt.Sprintf("%s %s", app, version),
			}
			const strPattern = "%-*s: %s"
			const longestLabelLength = 8
			if commit != "" {
				pieces = append(
					pieces,
					fmt.Sprintf(strPattern, longestLabelLength, "Commit", commit))
			}
			if builtBy != "" {
				pieces = append(
					pieces,
					fmt.Sprintf(strPattern, longestLabelLength, "Built by", builtBy))
			}
			if builtAt != "" {
				pieces = append(
					pieces,
					fmt.Sprintf(strPattern, longestLabelLength, "Built at", builtAt))
			}
			fmt.Println(strings.Join(pieces, "\n"))
		},
	}
}
