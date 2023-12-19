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

package stdlib

import "errors"

var ErrNotImplemented = errors.New("not implemented")
var ErrFloatValuesNotSupported = errors.New("float values are not yet supported by immudb")
var ErrTimeValuesNotSupported = errors.New("time values are not yet supported by immudb")
var ErrBadQueryString = errors.New("bad query string. use format immudb://username:secret@host:port/db")
