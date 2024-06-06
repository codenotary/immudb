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

// Package cmdtest ...
package cmdtest

import (
	"bytes"
	"io"
	"os"
)

// StdOutCollector ...
type StdOutCollector struct {
	CaptureStderr    bool
	realStdOut       *os.File
	fakeStdOutReader *os.File
	fakeStdOutWriter *os.File
}

// Start ...
func (c *StdOutCollector) Start() error {
	// keep backup of the real stdout/stderr
	if !c.CaptureStderr {
		c.realStdOut = os.Stdout
	} else {
		c.realStdOut = os.Stderr
	}
	var err error
	c.fakeStdOutReader, c.fakeStdOutWriter, err = os.Pipe()
	if err != nil {
		return err
	}
	if !c.CaptureStderr {
		os.Stdout = c.fakeStdOutWriter
	} else {
		os.Stderr = c.fakeStdOutWriter
	}
	return nil
}

// Stop ...
func (c *StdOutCollector) Stop() (string, error) {
	outC := make(chan string)
	outErr := make(chan error)
	// copy the output in a separate goroutine so printing can't block indefinitely
	go func() {
		var buf bytes.Buffer
		_, err := io.Copy(&buf, c.fakeStdOutReader)
		if err != nil {
			outErr <- err
		}
		outC <- buf.String()
	}()

	// back to normal state
	c.fakeStdOutWriter.Close()
	// restore the real stdout/stderr
	if !c.CaptureStderr {
		os.Stdout = c.realStdOut
	} else {
		os.Stderr = c.realStdOut
	}
	select {
	case out := <-outC:
		return out, nil
	case err := <-outErr:
		return "", err
	}
}
