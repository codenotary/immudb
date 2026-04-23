package runner

import (
	"context"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"

	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks"
	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks/readtxs"
	"github.com/codenotary/immudb/test/performance-test-suite/pkg/benchmarks/writetxs"
)

func SendResultsToInfluxDb(host string, token string, bucket string, runner string, version string, r *BenchmarkSuiteResult) {
	var client influxdb2.Client

	client = influxdb2.NewClient(host, token)

	writer := client.WriteAPIBlocking("Codenotary", bucket)

	for _, b := range r.Benchmarks {

		p := influxdb2.NewPointWithMeasurement("performance").
			AddTag("name", b.Name).
			AddTag("runner", runner).
			AddTag("version", version).
			AddField("duration", b.Duration.Seconds()).
			SetTime(b.EndTime)

		// Results is any of the per-benchmark Result types (writetxs,
		// readtxs, …). Type-switch to emit the fields each one
		// publishes. HWStats is the only field they all share.
		var hw *benchmarks.HWStats
		switch res := b.Results.(type) {
		case *writetxs.Result:
			p = p.
				AddField("txTotal", res.TxTotal).
				AddField("kvTotal", res.KvTotal).
				AddField("txs", res.Txs).
				AddField("kvs", res.Kvs)
			hw = res.HWStats
		case *readtxs.Result:
			p = p.
				AddField("getTotal", res.GetTotal).
				AddField("gets", res.Gets)
			hw = res.HWStats
		}
		if hw != nil {
			p = p.
				AddField("cpuTime", hw.CPUTime).
				AddField("vmm", hw.VMM).
				AddField("rss", hw.RSS).
				AddField("IOBytesWrite", hw.IOBytesWrite).
				AddField("IOBytesRead", hw.IOBytesRead).
				AddField("IOCallsRead", hw.IOCallsRead).
				AddField("IOCallsWrite", hw.IOCallsWrite)
		}

		writer.WritePoint(context.Background(), p)

	}

	client.Close()

}
