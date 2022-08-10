/*
Copyright 2022 Codenotary Inc. All rights reserved.

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

package logger

import (
	"errors"
	"log"
	"os"
	"path/filepath"
)

// FileLogger ...
type FileLogger struct {
	Logger   *log.Logger
	LogLevel LogLevel
	out      *os.File
}

// NewFileLogger ...
func NewFileLogger(name string, file string) (logger Logger, out *os.File, err error) {
	out, err = setup(file)
	if err != nil {
		return nil, nil, err
	}
	logger = &FileLogger{
		out:      out,
		Logger:   log.New(out, name, log.LstdFlags),
		LogLevel: LogLevelFromEnvironment(),
	}
	return logger, out, nil
}

// NewFileLoggerWithLevel ...
func NewFileLoggerWithLevel(name string, file string, level LogLevel) (logger Logger, err error) {
	out, err := setup(file)
	if err != nil {
		return nil, err
	}
	logger = &FileLogger{
		Logger:   log.New(out, name+".log", log.LstdFlags),
		LogLevel: level,
	}
	return logger, nil
}

func setup(file string) (out *os.File, err error) {
	if _, err = os.Stat(filepath.Dir(file)); os.IsNotExist(err) {
		if err = os.Mkdir(filepath.Dir(file), os.FileMode(0755)); err != nil {
			return nil, errors.New("Unable to create log folder")
		}
	}
	out, err = os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		return out, errors.New("Unable to create log file")
	}
	return out, err
}

// Errorf ...
func (l *FileLogger) Errorf(f string, v ...interface{}) {
	if l.LogLevel <= LogError {
		l.Logger.Printf("ERROR: "+f, v...)
	}
}

// Warningf ...
func (l *FileLogger) Warningf(f string, v ...interface{}) {
	if l.LogLevel <= LogWarn {
		l.Logger.Printf("WARNING: "+f, v...)
	}
}

// Infof ...
func (l *FileLogger) Infof(f string, v ...interface{}) {
	if l.LogLevel <= LogInfo {
		l.Logger.Printf("INFO: "+f, v...)
	}
}

// Debugf ...
func (l *FileLogger) Debugf(f string, v ...interface{}) {
	if l.LogLevel <= LogDebug {
		l.Logger.Printf("DEBUG: "+f, v...)
	}
}

// Close the logger ...
func (l *FileLogger) Close() error {
	if l.out != nil {
		return l.out.Close()
	}
	return nil
}
