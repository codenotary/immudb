/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package auth

import (
	"testing"
)

func TestHasPermissionForMethod(t *testing.T) {
	if !HasPermissionForMethod(PermissionSysAdmin, "DeactivateUser") {
		t.Errorf("HasPermissionForMethod error")
	}
	if HasPermissionForMethod(PermissionSysAdmin, "deactivateUser") {
		t.Errorf("HasPermissionForMethod error")
	}
	if HasPermissionForMethod(PermissionNone, "DeactivateUser") {
		t.Errorf("HasPermissionForMethod error")
	}

	if !HasPermissionForMethod(PermissionR, "CountAll") {
		t.Errorf("expected PermissionR to be sufficient for CountAll")
	}
	if HasPermissionForMethod(PermissionNone, "CountAll") {
		t.Errorf("expected PermissionNone to be insufficient for CountAll")
	}
}
