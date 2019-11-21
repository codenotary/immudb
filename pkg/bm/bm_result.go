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
