package client

import (
	"github.com/codenotary/immudb/pkg/logger"
)

type ImmuClient struct {
	Logger  logger.Logger
	Options *Options
}

func DefaultClient() *ImmuClient {
	return &ImmuClient{
		Logger: logger.DefaultLogger,
	}
}

func (c *ImmuClient) WithLogger(logger logger.Logger) *ImmuClient {
	c.Logger = logger
	return c
}

func (c *ImmuClient) WithOptions(options *Options) *ImmuClient {
	c.Options = options
	return c
}
