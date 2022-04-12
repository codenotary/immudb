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

package logger

import (
	"os"
	"strings"
)

// LogLevel ...
type LogLevel int8

// Log levels
const (
	LogDebug LogLevel = iota
	LogInfo
	LogWarn
	LogError
)

// Logger ...
type Logger interface {
	Errorf(string, ...interface{})
	Warningf(string, ...interface{})
	Infof(string, ...interface{})
	Debugf(string, ...interface{})
	CloneWithLevel(level LogLevel) Logger
}

func logLevelFromEnvironment() LogLevel {
	logLevel, _ := os.LookupEnv("LOG_LEVEL")
	switch strings.ToLower(logLevel) {
	case "error":
		return LogError
	case "warn":
		return LogWarn
	case "info":
		return LogInfo
	case "debug":
		return LogDebug
	}
	return LogInfo
}
