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

package logger

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

func createLogFileWriter(opts *Options) (io.Writer, error) {
	if opts.LogDir != "" {
		if err := os.MkdirAll(opts.LogDir, os.ModePerm); err != nil && !errors.Is(err, os.ErrExist) {
			return nil, err
		}
	}

	if opts.LogRotationAge > 0 && opts.LogRotationAge < logRotationAgeMin {
		return nil, fmt.Errorf("log rotation age must be greater than %s", logRotationAgeMin.String())
	}

	timeFunc := opts.TimeFnc
	if timeFunc == nil {
		timeFunc = func() time.Time {
			return time.Now()
		}
	}

	lf := &logFileWriter{
		timeFunc:        timeFunc,
		timeFormat:      opts.LogFileTimeFormat,
		dir:             opts.LogDir,
		fileName:        opts.LogFile,
		rotationSize:    opts.LogRotationSize,
		rotationAge:     opts.LogRotationAge,
		currSegmentSize: 0,
	}
	err := lf.rotate(lf.currAge())
	return lf, err
}

var _ io.Closer = (*logFileWriter)(nil)

type logFileWriter struct {
	currSegment *os.File

	timeFunc   TimeFunc
	timeFormat string

	dir      string
	fileName string

	rotationSize int
	rotationAge  time.Duration

	currentSegmentAgeNum int64
	currSegmentSize      int
	nextSeqNum           int
}

func (bf *logFileWriter) Write(buf []byte) (int, error) {
	age := bf.currAge()
	if bf.shouldRotate(len(buf), age) {
		if err := bf.rotate(age); err != nil {
			return -1, err
		}
	}

	n, err := bf.currSegment.Write(buf)
	if err == nil {
		bf.currSegmentSize += len(buf)
	}
	return n, err
}

func (bf *logFileWriter) shouldRotate(nBytes int, ageNum int64) bool {
	if bf.rotationAge == 0 && bf.rotationSize == 0 {
		return false
	}

	if bf.rotationSize > 0 && bf.currSegmentSize+int(nBytes) > bf.rotationSize {
		return true
	}

	if bf.rotationAge > 0 && int64(ageNum) > bf.currentSegmentAgeNum {
		return true
	}
	return false
}

func (bf *logFileWriter) rotate(age int64) error {
	if err := bf.Close(); err != nil {
		return err
	}

	bf.currSegmentSize = 0
	if bf.rotationAge > 0 {
		bf.currentSegmentAgeNum = age
	}

	name, err := bf.getNextSegmentName()
	if err != nil {
		return err
	}

	logFile, err := os.Create(name)
	if err != nil {
		return err
	}

	bf.currSegment = logFile
	return nil
}

func (bf *logFileWriter) getNextSegmentName() (string, error) {
	num := bf.nextSeqNum
	name := bf.segmentName()
	_, err := os.Stat(name)
	for err == nil {
		num++
		_, err = os.Stat(fmt.Sprintf("%s.%04d", name, num))
	}
	if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}

	// NOTE: Without specifying a time format, the same file names will be generated during each rotation.
	if bf.timeFormat == "" {
		bf.nextSeqNum = num
	}

	if num > 0 {
		return fmt.Sprintf("%s.%04d", name, num), nil
	}
	return name, nil
}

func (bf *logFileWriter) segmentName() string {
	if bf.timeFormat == "" || bf.rotationAge == 0 {
		return filepath.Join(bf.dir, bf.fileName)
	}

	ext := filepath.Ext(bf.fileName)
	t := time.Unix(0, bf.currentSegmentAgeNum*bf.rotationAge.Nanoseconds())

	name := fmt.Sprintf("%s_%s%s", strings.TrimSuffix(bf.fileName, ext), t.Format(bf.timeFormat), ext)
	return filepath.Join(bf.dir, name)
}

func (bf *logFileWriter) currAge() int64 {
	if bf.rotationAge == 0 {
		return 0
	}
	return bf.timeFunc().UnixNano() / bf.rotationAge.Nanoseconds()
}

func (bf *logFileWriter) Close() error {
	if bf.currSegment == nil {
		return nil
	}

	if err := bf.currSegment.Sync(); err != nil {
		return err
	}
	return bf.currSegment.Close()
}
