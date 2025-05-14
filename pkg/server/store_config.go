package server

import (
	"bufio"
	"os"

	"github.com/codenotary/immudb/v2/embedded/store"
	"github.com/spf13/viper"
)

type StoreConfig struct {
	NumIndexers                int `mapstructure:"num-indexers"`
	SharedWriteBufferSize      int `mapstructure:"shared-write-buffer-size"`
	SharedWriteBufferChunkSize int `mapstructure:"shared-write-buffer-chunk-size"`

	WriteBufferMinSize int `mapstructure:"write-buffer-min-size"`
	WriteBufferMaxSize int `mapstructure:"write-buffer-max-size"`
}

func parseStoreConfig(configPath string) (*StoreConfig, error) {
	viper := viper.New()

	viper.SetConfigName("config")
	viper.SetConfigType("toml")

	f, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if err := viper.ReadConfig(bufio.NewReader(f)); err != nil {
		return nil, err
	}

	type config struct {
		Store StoreConfig
	}

	var c config
	err = viper.Unmarshal(&c)
	if err != nil {
		return nil, err
	}
	return &c.Store, err
}

func getStoreOptionsFromConfig(configPath string) *store.Options {
	opts := store.DefaultOptions()

	conf, err := parseStoreConfig(configPath)
	if err != nil {
		return opts
	}

	if conf.NumIndexers > 0 {
		opts.IndexOpts.WithNumIndexers(conf.NumIndexers)
	}

	if conf.SharedWriteBufferChunkSize > 0 {
		opts.IndexOpts.WithSharedWriteBufferChunkSize(conf.SharedWriteBufferChunkSize)
	}

	if conf.SharedWriteBufferSize > 0 {
		opts.IndexOpts.WithSharedWriteBufferSize(conf.SharedWriteBufferSize)
	}

	if conf.WriteBufferMinSize > 0 {
		opts.IndexOpts.WithMinWriteBufferSize(conf.WriteBufferMinSize)
	}

	if conf.WriteBufferMaxSize > 0 {
		opts.IndexOpts.WithMaxWriteBufferSize(conf.WriteBufferMaxSize)
	}
	return opts
}
