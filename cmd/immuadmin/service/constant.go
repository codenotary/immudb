package service

import "errors"

var (
	// errUnsupportedSystem appears if try to use service on system which is not supported by this release
	ErrUnsupportedSystem = errors.New("unsupported system")

	// errRootPrivileges appears if run installation or deleting the service without root privileges
	ErrRootPrivileges = errors.New("you must have root user privileges. Possibly using 'sudo' command should help")

	// errExecNotFound provided executable file does not exists
	ErrExecNotFound = errors.New("executable file does not exists or not provided")
)
