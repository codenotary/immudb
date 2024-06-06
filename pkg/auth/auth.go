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

// Kind the authentication kind
type Kind uint32

// Authentication kinds
const (
	KindNone Kind = iota
	KindPassword
	KindCryptoSig
)

// TODO OGG: in the future, after other types of auth will be implemented,
// this will have to be of Kind (see above) type instead of bool:

// AuthEnabled toggles authentication on or off
var AuthEnabled bool

// DevMode if set to true, remote client commands (except admin ones) will be accepted even if auth is off
var DevMode bool

//IsTampered if set to true then one of the databases is tempered and the user is notified
var IsTampered bool

// WarnDefaultAdminPassword warning user message for the case when admin uses the default password
var WarnDefaultAdminPassword = "immudb user has the default password: please change it to ensure proper security"
