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

package helper

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"text/tabwriter"
)

// PrintTable prints data (string arrays) in a tabular format
func PrintTable(
	w io.Writer,
	cols []string,
	nbRows int,
	getRow func(int) []string,
	caption string,
) {
	if nbRows == 0 {
		return
	}
	nbCols := len(cols)
	if nbCols == 0 {
		return
	}
	colSep := "\t"

	maxNbDigits := 0
	tens := nbRows
	for tens != 0 {
		tens /= 10
		maxNbDigits++
	}
	header := append([]string{strings.Repeat("#", maxNbDigits)}, cols...)

	var sb strings.Builder
	for _, th := range header {
		for i := 0; i < len(th); i++ {
			sb.WriteString("-")
		}
		sb.WriteString(colSep)
	}
	borderBottom := sb.String()
	sb.Reset()
	if len(caption) <= 0 {
		caption = fmt.Sprintf("%d row(s)", nbRows)
	}
	fmt.Fprint(w, caption+"\n")

	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)
	fmt.Fprintln(tw, borderBottom)
	fmt.Fprint(tw, strings.Join(header, colSep), colSep, "\n")
	fmt.Fprintln(tw, borderBottom)
	for i := 0; i < nbRows; i++ {
		row := getRow(i)
		nbRowCols := len(row)
		for j := 0; j < nbCols; j++ {
			if j < nbRowCols {
				sb.WriteString(row[j])
			}
			sb.WriteString(colSep)
		}
		fmt.Fprint(tw, strconv.Itoa(i+1), colSep, sb.String(), "\n")
		sb.Reset()
	}
	fmt.Fprintln(tw, borderBottom)
	_ = tw.Flush()
}
