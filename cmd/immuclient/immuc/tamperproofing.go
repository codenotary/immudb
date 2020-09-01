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
	"context"
	"encoding/hex"
	"fmt"
	"strconv"

	"github.com/codenotary/immudb/pkg/api/schema"
)

func (i *immuc) Consistency(args []string) (string, error) {
	if len(args) < 2 {
		return "", fmt.Errorf("wrong number of arguments")
	}
	index, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	proof, err := i.ImmuClient.Consistency(ctx, index)
	if err != nil {
		return "", err
	}

	var root []byte
	src := []byte(args[1])
	l := hex.DecodedLen(len(src))
	if l != 32 {
		return "", err
	}
	root = make([]byte, l)
	_, err = hex.Decode(root, src)
	if err != nil {
		return "", err
	}

	str := fmt.Sprintf("verified: %t \nfirstRoot: %x at index: %d \nsecondRoot: %x at index: %d \n",
		proof.Verify(schema.Root{Payload: &schema.RootIndex{Index: index, Root: root}}),
		proof.FirstRoot,
		proof.First,
		proof.SecondRoot,
		proof.Second)
	return str, nil
}

func (i *immuc) Inclusion(args []string) (string, error) {
	index, err := strconv.ParseUint(args[0], 10, 64)
	if err != nil {
		return "", err
	}
	ctx := context.Background()
	proof, err := i.ImmuClient.Inclusion(ctx, index)
	if err != nil {
		return "", err
	}
	var hash []byte
	if len(args) > 1 {
		src := []byte(args[1])
		l := hex.DecodedLen(len(src))
		if l != 32 {
			return "", fmt.Errorf("invalid hash length")
		}
		hash = make([]byte, l)
		_, err = hex.Decode(hash, src)
		if err != nil {
			return "", err
		}
	} else {
		item, err := i.ImmuClient.ByIndex(ctx, index)
		if err != nil {
			return "", err
		}
		hash, err = item.Hash()
		if err != nil {
			return "", err
		}
	}
	str := fmt.Sprintf("verified: %t \nhash: %x at index: %d \nroot: %x at index: %d \n",
		proof.Verify(index, hash),
		proof.Leaf,
		proof.Index,
		proof.Root,
		proof.At)
	return str, nil
}
