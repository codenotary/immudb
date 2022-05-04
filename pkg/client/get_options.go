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

package client

import (
	"github.com/codenotary/immudb/pkg/api/schema"
)

type GetOption func(req *schema.KeyRequest) error

func NoWait(nowait bool) GetOption {
	return func(req *schema.KeyRequest) error {
		req.NoWait = nowait
		return nil
	}
}

func SinceTx(tx uint64) GetOption {
	return func(req *schema.KeyRequest) error {
		req.SinceTx = tx
		return nil
	}
}

func AtTx(tx uint64) GetOption {
	return func(req *schema.KeyRequest) error {
		req.AtTx = tx
		return nil
	}
}

func AtRevision(rev int64) GetOption {
	return func(req *schema.KeyRequest) error {
		req.AtRevision = rev
		return nil
	}
}
