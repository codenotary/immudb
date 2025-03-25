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

package sservice

//Option service instance options
type Option struct {
	ExecPath      string
	ConfigPath    string
	User          string
	Group         string
	UsageExamples string
	UsageDetails  string
	StartUpConfig string
	Config        []byte
}

//WithExecPath sets the exec path
func (o *Option) WithExecPath(path string) *Option {
	o.ExecPath = path
	return o
}

//WithConfigPath sets the config path
func (o *Option) WithConfigPath(path string) *Option {
	o.ConfigPath = path
	return o
}

//WithUser sets the user
func (o *Option) WithUser(user string) *Option {
	o.User = user
	return o
}

//WithGroup sets the groups
func (o *Option) WithGroup(group string) *Option {
	o.Group = group
	return o
}

//WithUsageExamples sets usage examples
func (o *Option) WithUsageExamples(usage string) *Option {
	o.UsageExamples = usage
	return o
}

//WithUsageDetails sets usage details
func (o *Option) WithUsageDetails(usage string) *Option {
	o.UsageDetails = usage
	return o
}

//WithStartUpConfig sets the startup configurations
func (o *Option) WithStartUpConfig(config string) *Option {
	o.StartUpConfig = config
	return o
}

//WithConfig sets the startup configurations
func (o *Option) WithConfig(config []byte) *Option {
	o.Config = config
	return o
}
