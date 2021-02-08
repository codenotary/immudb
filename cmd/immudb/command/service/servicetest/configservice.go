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

package servicetest

import (
	"io"

	"github.com/spf13/viper"
)

type ConfigServiceMock struct {
	*viper.Viper
}

func (v *ConfigServiceMock) WriteConfigAs(filename string) error {
	return nil
}
func (v *ConfigServiceMock) GetString(key string) string {
	return ""
}
func (v *ConfigServiceMock) SetConfigType(in string) {}

func (v *ConfigServiceMock) ReadConfig(in io.Reader) error {
	return nil
}
