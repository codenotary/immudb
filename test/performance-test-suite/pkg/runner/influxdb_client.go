package runner

import (
	"context"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func SendResultsToInfluxDb(host string, token string, bucket string, r *BenchmarkSuiteResult) {
	client := influxdb2.NewClient(host, token)
	writer := client.WriteAPIBlocking("Codenotary", bucket)

	for _, b := range r.Benchmarks {

		p := influxdb2.NewPointWithMeasurement("performance").
			AddTag("name", b.Name).
			AddField("duration", b.Duration.Seconds()).
			AddField("txTotal", b.Results.TxTotal).
			AddField("kvTotal", b.Results.KvTotal).
			AddField("txs", b.Results.Txs).
			AddField("kvs", b.Results.Kvs).
			AddField("cpuTime", b.Results.HWStats.CPUTime).
			AddField("vmm", b.Results.HWStats.VMM).
			AddField("rss", b.Results.HWStats.RSS).
			AddField("IOBytesWrite", b.Results.HWStats.IOBytesWrite).
			AddField("IOBytesRead", b.Results.HWStats.IOBytesRead).
			AddField("IOCallsRead", b.Results.HWStats.IOCallsRead).
			AddField("IOCallsWrite", b.Results.HWStats.IOCallsWrite).
			SetTime(time.Now())

		writer.WritePoint(context.Background(), p)

	}

	client.Close()

}
