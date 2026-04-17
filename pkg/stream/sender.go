/*
Copyright 2026 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package stream

import (
	"encoding/binary"
	"errors"
	"io"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// ErrSendStalled is returned by msgSender.Send when an individual chunk
// transmission exceeded the configured per-send timeout. Returning an error
// (instead of letting Send block until gRPC eventually unblocks) gives the
// caller a chance to abort, preventing unbounded growth of the gRPC
// per-stream buffer (~100MB default × N concurrent exporters = OOM risk).
var ErrSendStalled = errors.New("stream send stalled past configured timeout")

// streamSendBlockedSeconds is the histogram of time individual chunk Sends
// take. Buckets are biased toward sub-second values: a healthy receiver
// drains chunks in the µs range; values trending into the seconds bracket
// indicate a slow consumer that risks heap exhaustion through the gRPC
// per-stream window. Operators should alert on a non-zero p99 here.
var streamSendBlockedSeconds = promauto.NewHistogram(prometheus.HistogramOpts{
	Namespace: "immudb",
	Name:      "stream_send_blocked_duration_seconds",
	Help:      "Duration of individual stream chunk Send calls. Tail values indicate slow stream receivers.",
	Buckets: []float64{
		0.0001, 0.001, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30,
	},
})

type MsgSender interface {
	Send(reader io.Reader, chunkSize int, metadata map[string][]byte) (err error)
	RecvMsg(m interface{}) error
}

type msgSender struct {
	stream      ImmuServiceSender_Stream
	buf         []byte
	chunk       *schema.Chunk
	sendTimeout time.Duration
}

// MsgSenderOption configures an msgSender at construction.
type MsgSenderOption func(*msgSender)

// WithSendTimeout enables A5 back-pressure: if any single chunk Send takes
// longer than d, sendChunk returns ErrSendStalled. The blocked goroutine
// is left running (gRPC Send has no cancellation primitive); when it
// eventually completes, its result is discarded. Use only when the caller
// is prepared to abandon the stream on stall — otherwise prefer the
// uncapped default and rely on the histogram for visibility.
func WithSendTimeout(d time.Duration) MsgSenderOption {
	return func(s *msgSender) {
		s.sendTimeout = d
	}
}

// NewMsgSender returns a NewMsgSender. It can be used on server side or client side to send a message on a stream.
func NewMsgSender(s ImmuServiceSender_Stream, buf []byte, opts ...MsgSenderOption) *msgSender {
	ms := &msgSender{
		stream: s,
		buf:    buf,
		chunk:  &schema.Chunk{},
	}
	for _, opt := range opts {
		opt(ms)
	}
	return ms
}

// sendChunk wraps st.stream.Send with two A5 instrumentation hooks:
//
//  1. Always observes the elapsed Send duration into
//     streamSendBlockedSeconds so operators get visibility on receiver
//     stall trends without configuration.
//  2. When sendTimeout is non-zero, races the Send against a timer and
//     returns ErrSendStalled if the timer wins. The Send goroutine is
//     leaked deliberately — gRPC offers no cancellation for an in-flight
//     Send and forcing the stream to close can corrupt subsequent chunks.
//     The blocked goroutine completes when gRPC eventually accepts the
//     write; its result is discarded.
func (st *msgSender) sendChunk(c *schema.Chunk) error {
	start := time.Now()

	if st.sendTimeout <= 0 {
		err := st.stream.Send(c)
		streamSendBlockedSeconds.Observe(time.Since(start).Seconds())
		return err
	}

	done := make(chan error, 1)
	go func() {
		done <- st.stream.Send(c)
	}()

	timer := time.NewTimer(st.sendTimeout)
	defer timer.Stop()

	select {
	case err := <-done:
		streamSendBlockedSeconds.Observe(time.Since(start).Seconds())
		return err
	case <-timer.C:
		streamSendBlockedSeconds.Observe(time.Since(start).Seconds())
		return ErrSendStalled
	}
}

// Send reads from a reader until it reach msgSize. It fill an internal buffer from what it read from reader and, when there is enough data, it sends a chunk on stream.
// It continues until it reach the msgSize. At that point it sends the last content of the buffer.
func (st *msgSender) Send(reader io.Reader, msgSize int, metadata map[string][]byte) error {
	available := len(st.buf)

	// first chunk begins with the message size and including metadata
	binary.BigEndian.PutUint64(st.buf, uint64(msgSize))
	available -= 8

	st.chunk.Metadata = metadata

	read := 0

	for read < msgSize {
		n, err := reader.Read(st.buf[len(st.buf)-available:])
		if err != nil {
			return err
		}

		available -= n
		read += n

		if available == 0 {
			// send chunk when it's full
			st.chunk.Content = st.buf[:len(st.buf)-available]

			err = st.sendChunk(st.chunk)
			if err != nil {
				return err
			}

			available = len(st.buf)

			// metadata is only included into the first chunk
			st.chunk.Metadata = nil
		}
	}

	if available < len(st.buf) {
		// send last partially written chunk
		st.chunk.Content = st.buf[:len(st.buf)-available]

		err := st.sendChunk(st.chunk)
		if err != nil {
			return err
		}

		// just to avoid keeping a useless reference
		st.chunk.Metadata = nil
	}

	return nil
}

// RecvMsg block until it receives a message from the receiver (here we are on the sender). It's used mainly to retrieve an error message after sending data from a client(SDK) perspective.
func (st *msgSender) RecvMsg(m interface{}) error {
	return st.stream.RecvMsg(m)
}
