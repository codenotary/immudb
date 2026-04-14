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

package audit

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/database"
)

// QueryOptions configures audit event queries.
type QueryOptions struct {
	SinceTimestamp int64  // UnixNano, inclusive (0 = from beginning)
	UntilTimestamp int64  // UnixNano, inclusive (0 = no upper bound)
	Limit          uint64 // Max events to return (0 = default 100)
	Desc           bool   // Reverse chronological order
}

// Query scans audit events from the database by prefix.
func Query(ctx context.Context, db database.DB, opts QueryOptions) ([]*AuditEvent, error) {
	limit := opts.Limit
	if limit == 0 {
		limit = 100
	}

	seekKey := []byte(KeyPrefix)
	if opts.SinceTimestamp > 0 {
		seekKey = []byte(fmt.Sprintf("%s%020d", KeyPrefix, opts.SinceTimestamp))
	}

	entries, err := db.Scan(ctx, &schema.ScanRequest{
		Prefix:  []byte(KeyPrefix),
		SeekKey: seekKey,
		Limit:   limit,
		Desc:    opts.Desc,
		NoWait:  true,
	})
	if err != nil {
		return nil, fmt.Errorf("audit query scan: %w", err)
	}

	var events []*AuditEvent
	for _, entry := range entries.Entries {
		var event AuditEvent
		if err := json.Unmarshal(entry.Value, &event); err != nil {
			continue
		}

		if opts.UntilTimestamp > 0 && event.Timestamp > opts.UntilTimestamp {
			break
		}

		events = append(events, &event)
	}

	return events, nil
}
