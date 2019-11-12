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
