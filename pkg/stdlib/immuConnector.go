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

package stdlib

import (
	"context"
	"database/sql/driver"
	"github.com/codenotary/immudb/pkg/client"
)

type immuConnector struct {
	cliOptions *client.Options
	driver     *Driver
}

func (c immuConnector) Driver() driver.Driver {
	return c.driver
}

func (c immuConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.driver.getNewConnByOptions(ctx, c.cliOptions)
}
