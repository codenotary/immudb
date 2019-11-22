/*
Copyright 2019 vChain, Inc.

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

package bm

import (
	"fmt"
)

type BmResult struct {
	Bm           *Bm
	Time         float64
	Transactions float64
}

func (b BmResult) String() string {
	return fmt.Sprintf(
		`
Name:		%s
Concurency:	%d
Iterations:	%d
Elapsed t.:	%.2f sec
Throughput:	%.0f tx/sec
`,
		b.Bm.Name, b.Bm.Concurrency, b.Bm.Iterations, b.Time, b.Transactions)
}
