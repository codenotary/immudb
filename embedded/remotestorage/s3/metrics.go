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
package s3

import (
	"io"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	metricsUploadBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "immudb_remoteapp_s3_upload_bytes",
		Help: "Number data bytes (excluding headers) uploaded to s3",
	})

	metricsDownloadBytes = promauto.NewCounter(prometheus.CounterOpts{
		Name: "immudb_remoteapp_s3_download_bytes",
		Help: "Number data bytes (excluding headers) downloaded from s3",
	})
)

type metricsCountingReadCloser struct {
	r io.ReadCloser
	c prometheus.Counter
}

func (m *metricsCountingReadCloser) Read(b []byte) (int, error) {
	n, err := m.r.Read(b)
	m.c.Add(float64(n))
	return n, err
}

func (m *metricsCountingReadCloser) Close() error {
	return m.r.Close()
}
