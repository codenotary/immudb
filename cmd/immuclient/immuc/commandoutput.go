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

package immuc

import (
	"encoding/hex"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/olekukonko/tablewriter"
)

// CommandOutput represents an output from a command
type CommandOutput interface {
	// Render the output as a string
	Plain() string

	// Get only the essential value
	ValueOnly() string

	// Render the output as JSON object to marshal
	Json() interface{}
}

// errorOutput contains error information
type errorOutput struct {
	err string
}

func (o *errorOutput) ValueOnly() string { return o.err }
func (o *errorOutput) Plain() string     { return o.err }
func (o *errorOutput) Json() interface{} { return map[string]string{"error": o.err} }

// resultOutput contains a single value output (operation result)
type resultOutput struct {
	Result  interface{} `json:"result"`
	Warning string      `json:"warning,omitempty"`
}

func (s *resultOutput) Plain() string {
	if s.Warning != "" {
		return fmt.Sprintf("%v\n%s", s.Result, s.Warning)
	}
	return fmt.Sprint(s.Result)
}

func (s *resultOutput) ValueOnly() string {
	return fmt.Sprint(s.Result)
}

func (s *resultOutput) Json() interface{} {
	return s
}

// currentStateOutput contains the result of the db state call
type currentStateOutput struct {
	Db     string `json:"database"`
	TxId   uint64 `json:"txID"`
	TxHash string `json:"hash,omitempty"`
}

func (o *currentStateOutput) Plain() string {
	if o.TxId == 0 {
		return fmt.Sprintf("database '%s' is empty", o.Db)
	}

	str := &strings.Builder{}

	fmt.Fprintf(str, "database:	%s\n", o.Db)
	fmt.Fprintf(str, "txID:		%d\n", o.TxId)
	fmt.Fprintf(str, "hash:		%s", o.TxHash)

	return str.String()
}

func (o *currentStateOutput) ValueOnly() string { return o.Plain() }
func (o *currentStateOutput) Json() interface{} { return o }

// kvOutput contains a single KV entry output
type kvOutput struct {
	entry    *schema.Entry
	verified bool
}

func (o *kvOutput) Plain() string {
	str := &strings.Builder{}
	o.writePlain(str)
	return str.String()
}

func (o *kvOutput) writePlain(str io.Writer) {
	fmt.Fprintf(str, "tx:       %d\n", o.entry.Tx)

	if o.entry.Revision != 0 {
		fmt.Fprintf(str, "rev:      %d\n", o.entry.Revision)
	}

	fmt.Fprintf(str, "key:      %s\n", o.entry.Key)

	if o.entry.Metadata != nil {
		fmt.Fprintf(str, "metadata: {%s}\n", o.entry.Metadata)
	}

	fmt.Fprintf(str, "value:    %s\n", o.entry.Value)

	if o.verified {
		fmt.Fprintf(str, "verified: %t\n", o.verified)
	}
}

func (o *kvOutput) ValueOnly() string {
	return string(o.entry.Value)
}

func (o *kvOutput) Json() interface{} {
	return &struct {
		Key      string             `json:"key"`
		Value    string             `json:"value"`
		Tx       uint64             `json:"tx"`
		Revision uint64             `json:"revision,omitempty"`
		Verified bool               `json:"verified,omitempty"`
		Metadata *schema.KVMetadata `json:"metadata,omitempty"`
	}{
		Key:      string(o.entry.Key),
		Value:    string(o.entry.Value),
		Tx:       o.entry.Tx,
		Revision: o.entry.Revision,
		Verified: o.verified,
		Metadata: o.entry.Metadata,
	}
}

// multiKVOutput contains KV output for multiple entries
type multiKVOutput struct {
	entries []kvOutput
}

func (o *multiKVOutput) Plain() string {
	str := &strings.Builder{}
	for i, entry := range o.entries {
		if i > 0 {
			str.WriteString("\n")
		}
		entry.writePlain(str)
	}
	return str.String()
}

func (o *multiKVOutput) ValueOnly() string {
	str := &strings.Builder{}
	for i, entry := range o.entries {
		if i > 0 {
			str.WriteString("\n")
		}
		str.Write(entry.entry.Value)
	}
	return str.String()
}

func (o *multiKVOutput) Json() interface{} {
	ret := &struct {
		Items []interface{} `json:"items"`
	}{}

	for _, entry := range o.entries {
		ret.Items = append(ret.Items, entry.Json())
	}

	return ret
}

// zEntryOutput contains a single ZSet entry output
type zEntryOutput struct {
	set           []byte
	referencedKey []byte
	score         float64
	txhdr         *schema.TxHeader
	verified      bool
}

func (o *zEntryOutput) Plain() string {
	str := &strings.Builder{}
	o.writePlain(str)
	return str.String()
}

func (o *zEntryOutput) writePlain(str io.Writer) {
	fmt.Fprintf(str,
		""+
			"tx:             %d\n"+
			"set:            %s\n"+
			"referenced key: %s\n"+
			"score:          %f\n"+
			"hash:           %x\n"+
			"verified:       %t\n",
		o.txhdr.Id,
		o.set,
		o.referencedKey,
		o.score,
		o.txhdr.EH,
		o.verified,
	)
}

func (o *zEntryOutput) ValueOnly() string {
	return string(o.referencedKey)
}

func (o *zEntryOutput) Json() interface{} {
	return &struct {
		Set           string  `json:"set"`
		ReferencedKey string  `json:"referencedKey"`
		Tx            uint64  `json:"tx"`
		Score         float64 `json:"score"`
		Hash          string  `json:"hash"`
		Verified      bool    `json:"verified,omitempty"`
	}{
		Set:           string(o.set),
		ReferencedKey: string(o.referencedKey),
		Tx:            o.txhdr.Id,
		Score:         o.score,
		Hash:          hex.EncodeToString(o.txhdr.EH),
		Verified:      o.verified,
	}
}

// healthOutput represents output of a health operation
type healthOutput struct {
	h *schema.DatabaseHealthResponse
}

func (o *healthOutput) Plain() string {
	return fmt.Sprintf(""+
		"pendingRequests:        %d\n"+
		"lastRequestCompletedAt: %s\n",
		o.h.PendingRequests,
		time.Unix(0, o.h.LastRequestCompletedAt*int64(time.Millisecond)),
	)
}

func (o *healthOutput) ValueOnly() string {
	return o.Plain()
}

func (o *healthOutput) Json() interface{} {
	lastCompleted := time.Unix(
		0,
		o.h.LastRequestCompletedAt*int64(time.Millisecond),
	).UTC().Format(
		time.RFC3339Nano,
	)
	return &struct {
		PendingRequests        uint32 `json:"pendingRequests"`
		LastRequestCompletedAt string `json:"lastRequestCompletedAt"`
	}{
		PendingRequests:        o.h.PendingRequests,
		LastRequestCompletedAt: lastCompleted,
	}
}

// txInfoOutput represents output of a health operation
type txInfoOutput struct {
	tx       *schema.Tx
	verified bool
}

func (o *txInfoOutput) Plain() string {
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("tx:		%d\n", o.tx.Header.Id))
	str.WriteString(fmt.Sprintf("time:		%s\n", time.Unix(int64(o.tx.Header.Ts), 0)))
	str.WriteString(fmt.Sprintf("entries:	%d\n", o.tx.Header.Nentries))
	str.WriteString(fmt.Sprintf("hash:		%x\n", schema.TxHeaderFromProto(o.tx.Header).Alh()))
	if o.verified {
		str.WriteString(fmt.Sprintf("verified:	%t \n", o.verified))
	}

	return str.String()
}

func (o *txInfoOutput) ValueOnly() string {
	return o.Plain()
}

func (o *txInfoOutput) Json() interface{} {
	time := time.Unix(int64(o.tx.Header.Ts), 0).UTC().Format(time.RFC3339)
	hash := schema.TxHeaderFromProto(o.tx.Header).Alh()
	return &struct {
		Tx       uint64 `json:"tx"`
		Time     string `json:"time"`
		Entries  int32  `json:"entriesCount"`
		Hash     string `json:"hash"`
		Verified bool   `json:"verified,omitempty"`
	}{
		Tx:       o.tx.Header.Id,
		Time:     time,
		Entries:  o.tx.Header.Nentries,
		Hash:     hex.EncodeToString(hash[:]),
		Verified: o.verified,
	}
}

// sqlExecOutput represents output of an sqlExec operation
type sqlExecOutput struct {
	UpdatedRows int `json:"updatedRows"`
}

func (o *sqlExecOutput) Plain() string {
	return fmt.Sprintf("Updated rows: %d", o.UpdatedRows)
}

func (o *sqlExecOutput) ValueOnly() string {
	return fmt.Sprintf("%d", o.UpdatedRows)
}

func (o *sqlExecOutput) Json() interface{} {
	return o
}

// tableOutput represents a common tabular output
type tableOutput struct {
	resp *schema.SQLQueryResult
}

func (o *tableOutput) Plain() string {
	result := &strings.Builder{}
	consoleTable := tablewriter.NewWriter(result)
	cols := make([]string, len(o.resp.Columns))
	for i, c := range o.resp.Columns {
		cols[i] = c.Name
	}
	consoleTable.SetHeader(cols)

	for _, r := range o.resp.Rows {
		row := make([]string, len(r.Values))

		for i, v := range r.Values {
			row[i] = schema.RenderValue(v.Value)
		}

		consoleTable.Append(row)
	}

	consoleTable.Render()
	return result.String()
}

func (o *tableOutput) ValueOnly() string {
	result := &strings.Builder{}

	for i, r := range o.resp.Rows {
		if i > 0 {
			result.WriteString("\n")
		}
		for j, v := range r.Values {
			if j > 0 {
				result.WriteString(",")
			}
			result.WriteString(schema.RenderValue(v.Value))
		}
	}

	return result.String()
}

func (o *tableOutput) Json() interface{} {
	rows := []map[string]interface{}{}
	for _, row := range o.resp.Rows {
		r := map[string]interface{}{}
		for i, v := range row.Values {
			if i > len(o.resp.Columns) {
				r[fmt.Sprintf("UNKNOWN_COLUMN_%d", i+1)] = schema.RawValue(v)
			} else {
				r[o.resp.Columns[i].Name] = schema.RawValue(v)
			}
		}
		rows = append(rows, r)
	}

	return map[string]interface{}{
		"rows": rows,
	}
}
