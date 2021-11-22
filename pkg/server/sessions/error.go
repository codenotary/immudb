/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

package sessions

import (
	"github.com/codenotary/immudb/pkg/errors"
)

var ErrSessionAlreadyPresent = errors.New("session already present").WithCode(errors.CodInternalError)
var ErrNoSessionIDPresent = errors.New("no sessionID provided").WithCode(errors.CodInvalidAuthorizationSpecification)
var ErrNoSessionAuthDataProvided = errors.New("no session auth data provided").WithCode(errors.CodInvalidAuthorizationSpecification)
var ErrSessionNotFound = errors.New("no session found").WithCode(errors.CodInvalidParameterValue)
