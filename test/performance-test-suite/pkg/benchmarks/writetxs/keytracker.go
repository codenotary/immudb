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
package writetxs

import (
	"fmt"
	"math/rand"
	"sync/atomic"
)

type keyTracker struct {
	start int
	max   int64
}

func (kt *keyTracker) getWKey() string {
	max := atomic.AddInt64(&kt.max, 1)
	return fmt.Sprintf("KEY:%10d", max+int64(kt.start))
}

func (kt *keyTracker) getRKey() string {
	max := atomic.LoadInt64(&kt.max)

	k := rand.Intn(int(max)-kt.start) + kt.start
	return fmt.Sprintf("KEY:%10d", k)
}
