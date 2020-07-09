package servicetest

import (
	"os"
	"os/user"
)

type Ossmock struct{}

func (oss Ossmock) LookupGroup(name string) (*user.Group, error) {
	return &user.Group{}, nil
}

func (oss Ossmock) Lookup(username string) (*user.User, error) {
	return &user.User{}, nil
}

func (oss Ossmock) Chown(name string, uid, gid int) error {
	return nil
}

func (oss Ossmock) MkdirAll(path string, perm os.FileMode) error {
	return nil
}
