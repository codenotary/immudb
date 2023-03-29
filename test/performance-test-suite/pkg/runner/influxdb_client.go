package runner

import (
	"context"
	"net/http"
	"net/url"
	"os"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
)

func SendResultsToInfluxDb(host string, token string, bucket string, runner string, version string, r *BenchmarkSuiteResult) {
	var client influxdb2.Client

	if runner == "benchmark" {
		proxyUrl, _ := url.Parse(os.Getenv("HTTPS_PROXY"))
		httpClient := &http.Client{Transport: &http.Transport{Proxy: http.ProxyURL(proxyUrl)}}
		client = influxdb2.NewClientWithOptions(host, token, influxdb2.DefaultOptions().SetHTTPClient(httpClient))
	} else {
		client = influxdb2.NewClient(host, token)
	}

	writer := client.WriteAPIBlocking("Codenotary", bucket)

	for _, b := range r.Benchmarks {

		p := influxdb2.NewPointWithMeasurement("performance").
			AddTag("name", b.Name).
			AddTag("runner", runner).
			AddTag("version", version).
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
			SetTime(b.EndTime)

		writer.WritePoint(context.Background(), p)

	}

	client.Close()

}
