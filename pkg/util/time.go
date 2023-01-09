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

package util

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"time"
)

// valid matches are up to minute level (y,w,d,h,m)
var validDurationRE = regexp.MustCompile("^(([0-9]+)y)?(([0-9]+)w)?(([0-9]+)d)?(([0-9]+)h)?(([0-9]+)m)?$")

// ParseDuration parses a string into a time.Duration
func ParseDuration(p string) (time.Duration, error) {
	if p == "" {
		return 0, errors.New("invalid duration")
	}

	m := validDurationRE.FindStringSubmatch(p)
	if m == nil {
		return 0, fmt.Errorf("invalid duration: %q", p)
	}

	var d time.Duration

	minute := time.Duration(1 * 60)
	hour := time.Duration(minute * 60)
	day := time.Duration(hour * 24)
	week := time.Duration(day * 7)
	year := time.Duration(day * 365)

	d += match(m[2], year)    // y
	d += match(m[4], week)    // w
	d += match(m[6], day)     // d
	d += match(m[8], hour)    // h
	d += match(m[10], minute) // m

	return d, nil
}

func match(val string, mult time.Duration) time.Duration {
	if val == "" {
		return 0
	}

	v, _ := strconv.Atoi(val)
	d := time.Duration(v) * time.Second
	return d * mult
}
