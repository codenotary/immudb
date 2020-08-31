/*
Copyright 2019-2020 vChain, Inc.

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
	"fmt"
	"strings"
	"time"

	"github.com/codenotary/immudb/pkg/api"
	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/client"
	"github.com/codenotary/immudb/pkg/store"
)

// PrintItem ...
func PrintItem(key []byte, value []byte, message interface{}, valueOnly bool) string {
	var index, ts uint64
	var verified, isVerified bool
	var hash []byte
	switch m := message.(type) {
	case *schema.Index:
		index = m.Index
		dig := api.Digest(index, key, value)
		hash = dig[:]
	case *client.VerifiedIndex:
		index = m.Index
		dig := api.Digest(index, key, value)
		hash = dig[:]
		verified = m.Verified
		isVerified = true
	case *schema.Item:
		key = m.Key
		value = m.Value
		index = m.Index
		hash = m.Hash()
	case *schema.StructuredItem:
		key = m.Key
		value = m.Value.Payload
		ts = m.Value.Timestamp
		index = m.Index
		hash, _ = m.Hash()
	case *client.VerifiedItem:
		key = m.Key
		value = m.Value
		index = m.Index
		ts = m.Time
		verified = m.Verified
		isVerified = true
		me, _ := schema.Merge(value, ts)
		dig := api.Digest(index, key, me)
		hash = dig[:]

	}
	if valueOnly {
		return fmt.Sprintf("%s\n", value)
	}
	str := strings.Builder{}
	if !valueOnly {
		str.WriteString(fmt.Sprintf("index:		%d \n", index))
		str.WriteString(fmt.Sprintf("key:		%s \n", key))
		str.WriteString(fmt.Sprintf("value:		%s \n", value))
		str.WriteString(fmt.Sprintf("hash:		%x \n", hash))
		str.WriteString(fmt.Sprintf("time:		%s \n", time.Unix(int64(ts), 0)))
		if isVerified {
			str.WriteString(fmt.Sprintf("verified:	%t \n", verified))
		}
	}

	return str.String()
}

// PrintSetItem ...
func PrintSetItem(set []byte, rkey []byte, score float64, message interface{}) string {
	var index uint64
	var verified, isVerified bool
	switch m := message.(type) {
	case *schema.Index:
		index = m.Index
	case *client.VerifiedIndex:
		index = m.Index
		verified = m.Verified
		isVerified = true
	}
	key, err := store.SetKey(rkey, set, score)
	if err != nil {
		return fmt.Sprint(err.Error())
	}
	if !isVerified {
		return fmt.Sprintf("index:		%d\nset:		%s \nkey:		%s \nscore:		%f \nvalue:		%s \nhash:		%x \n",
			index,
			set,
			key,
			score,
			rkey,
			api.Digest(index, key, rkey))
	}
	return fmt.Sprintf("index:		%d\nset:		%s\nkey:		%s\nscore:		%f\nvalue:		%s\nhash:		%x\nverified:	%t\n",
		index,
		set,
		key,
		score,
		rkey,
		api.Digest(index, key, rkey),
		verified)
}

// PrintRoot ...
func PrintRoot(root *schema.Root) string {
	if root.Payload.Root == nil {
		return "immudb is empty\n"
	}
	return fmt.Sprintf("index:		%d\nhash:		%x\n", root.Payload.Index, root.Payload.Root)
}

// PrintByIndex ...
func PrintByIndex(item *schema.StructuredItem, valueOnly bool) string {
	dig, _ := item.Hash()
	if valueOnly {
		return fmt.Sprintf("%s\n", item.Value)
	}
	str := strings.Builder{}
	str.WriteString(fmt.Sprintf("index:		%d\n", item.Index))
	str.WriteString(fmt.Sprintf("key:		%s\n", item.Key))
	str.WriteString(fmt.Sprintf("value:		%s\n", item.Value))
	str.WriteString(fmt.Sprintf("hash:		%x\n", dig))
	str.WriteString(fmt.Sprintf("time:		%s\n", time.Unix(int64(item.Value.Timestamp), 0)))

	return str.String()
}

// PadRight ...
func PadRight(str, pad string, length int) string {
	for {
		str += pad
		if len(str) > length {
			return str[0:length]
		}
	}
}
