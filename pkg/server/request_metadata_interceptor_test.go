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

package server

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

// BenchmarkIPAddrFromContext measures per-RPC cost of ipAddrFromContext, which
// is invoked from every audit-log and access-log interceptor (and the
// request-metadata interceptor). It isn't free: we call p.Addr.String() (heap
// allocation) and scan for ':'. This bench exists to gate future work that
// caches the host in the context or fans it out from a single first-run
// interceptor.
func BenchmarkIPAddrFromContext(b *testing.B) {
	cases := []struct {
		name string
		ctx  func() context.Context
	}{
		{
			name: "peer_only_ipv4",
			ctx: func() context.Context {
				return peer.NewContext(context.Background(), &peer.Peer{
					Addr: &net.TCPAddr{IP: net.ParseIP("10.0.0.42"), Port: 57382},
				})
			},
		},
		{
			name: "peer_only_ipv6",
			ctx: func() context.Context {
				return peer.NewContext(context.Background(), &peer.Peer{
					Addr: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 57382},
				})
			},
		},
		{
			name: "x_forwarded_for",
			ctx: func() context.Context {
				md := metadata.New(map[string]string{
					"x-forwarded-for": "203.0.113.7",
				})
				ctx := metadata.NewIncomingContext(context.Background(), md)
				return peer.NewContext(ctx, &peer.Peer{
					Addr: &net.TCPAddr{IP: net.ParseIP("10.0.0.42"), Port: 57382},
				})
			},
		},
	}

	for _, tc := range cases {
		ctx := tc.ctx()
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = ipAddrFromContext(ctx)
			}
		})
	}
}
