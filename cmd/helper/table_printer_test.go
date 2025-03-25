/*
Copyright 2025 Codenotary Inc. All rights reserved.

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
	"os"
	"testing"

	"github.com/codenotary/immudb/cmd/cmdtest"
	"github.com/stretchr/testify/assert"
)

func TestPrintTable(t *testing.T) {
	collector := new(cmdtest.StdOutCollector)
	collector.Start()
	elements := make([]string, 2)
	elements[0] = "one"
	elements[1] = "two"
	PrintTable(
		os.Stdout,
		[]string{"Database Name"},
		len(elements),
		func(i int) []string {
			row := make([]string, 1)
			row[0] = elements[i]
			return row
		},
		"",
	)
	ris, _ := collector.Stop()
	assert.Contains(t, ris, "one")
	assert.Contains(t, ris, "two")
	assert.Contains(t, ris, "2 row(s)")

	// custom table caption
	elements[1] = "three"
	collector.Start()
	PrintTable(
		os.Stdout,
		[]string{"Database Name"},
		len(elements),
		func(i int) []string {
			row := make([]string, 1)
			row[0] = elements[i]
			return row
		},
		"2 numbers",
	)
	ris, _ = collector.Stop()
	assert.Contains(t, ris, "three")
	assert.Contains(t, ris, "2 numbers")
}

func TestPrintTableZeroEle(t *testing.T) {
	collector := new(cmdtest.StdOutCollector)
	collector.Start()
	elements := make([]string, 0)
	PrintTable(
		os.Stdout,
		[]string{"Database Name"},
		len(elements),
		func(i int) []string {
			row := make([]string, 1)
			row[0] = elements[i]
			return row
		},
		"",
	)
	ris, _ := collector.Stop()
	assert.Equal(t, "", ris)
}

func TestPrintTableZeroCol(t *testing.T) {
	collector := new(cmdtest.StdOutCollector)
	collector.Start()
	elements := make([]string, 2)
	elements[0] = "one"
	elements[1] = "two"
	PrintTable(
		os.Stdout,
		[]string{},
		len(elements),
		func(i int) []string {
			row := make([]string, 1)
			row[0] = elements[i]
			return row
		},
		"",
	)
	ris, _ := collector.Stop()
	assert.Equal(t, "", ris)
}
