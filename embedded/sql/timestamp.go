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

package sql

import "time"

func TimeToInt64(t time.Time) int64 {
	unix := t.Unix()
	nano := t.Nanosecond()

	return unix*1e6 + int64(nano)/1e3
}

func TimeFromInt64(t int64) time.Time {
	return time.Unix(t/1e6, (t%1e6)*1e3).UTC()
}
