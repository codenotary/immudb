/*
Copyright 2025 Codenotary Inc. All rights reserved.

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

package immuc

import (
	"github.com/codenotary/immudb/pkg/client"
)

type Options struct {
	immudbClientOptions *client.Options
	valueOnly           bool
	revisionSeparator   string
}

func (o *Options) GetImmudbClientOptions() *client.Options {
	return o.immudbClientOptions
}

func (o *Options) WithImmudbClientOptions(opts *client.Options) *Options {
	o.immudbClientOptions = opts
	return o
}

func (o *Options) GetValueOnly() bool {
	return o.valueOnly
}

func (o *Options) WithValueOnly(valueOnly bool) *Options {
	o.valueOnly = valueOnly
	return o
}

func (o *Options) GetRevisionSeparator() string {
	return o.revisionSeparator
}

func (o *Options) WithRevisionSeparator(revisionSeparator string) *Options {
	o.revisionSeparator = revisionSeparator
	return o
}
