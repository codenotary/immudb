package servicetest

import (
	"github.com/spf13/viper"
	"io"
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
