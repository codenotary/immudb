/*
Copyright 2019 vChain, Inc.

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

func (c *ImmuClient) Errorf(f string, v ...interface{}) {
	if c.Logger != nil {
		c.Logger.Errorf(f, v...)
	}
}

func (c *ImmuClient) Warningf(f string, v ...interface{}) {
	if c.Logger != nil {
		c.Logger.Warningf(f, v...)
	}
}

func (c *ImmuClient) Infof(f string, v ...interface{}) {
	if c.Logger == nil {
		c.Logger.Infof(f, v...)
	}
}

func (c *ImmuClient) Debugf(f string, v ...interface{}) {
	if c.Logger == nil {
		c.Logger.Debugf(f, v...)
	}
}
