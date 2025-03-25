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

package stream

import "testing"

func TestIsOps(t *testing.T) {
	opZAdd := &Op_ZAdd{}
	opZAdd.isOp_Operation()
	opRef := &Op_Ref{}
	opRef.isOp_Operation()
	opKV := &Op_KeyValue{}
	opKV.isOp_Operation()
}
