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

package stdlib

import (
	"context"
	"database/sql/driver"
)

type driverConnector struct {
	name   string
	driver *Driver
}

// Connect implement driver.Connector interface
func (c *driverConnector) Connect(ctx context.Context) (conn driver.Conn, err error) {
	c.driver.configMutex.Lock()
	immuClientOption := c.driver.clientOptions[c.name]
	c.driver.configMutex.Unlock()

	if immuClientOption == nil {
		immuClientOption, err = ParseConfig(c.name)
		if err != nil {
			return nil, err
		}
	}
	return c.driver.getNewConnByOptions(ctx, immuClientOption)
}

func (dc *driverConnector) Driver() driver.Driver {
	return dc.driver
}
