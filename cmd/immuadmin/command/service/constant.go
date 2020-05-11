package service

import "errors"

var (
	// ErrUnsupportedSystem appears if try to use service on system which is not supported by this release
	ErrUnsupportedSystem = errors.New("unsupported system")

	// ErrRootPrivileges appears if run installation or deleting the service without root privileges
	ErrRootPrivileges = errors.New("you must have root user privileges. Possibly using 'sudo' command should help")

	// ErrExecNotFound provided executable file does not exists
	ErrExecNotFound = errors.New("executable file does not exists or not provided")

	// ErrServiceNotInstalled provided executable file does not exists
	ErrServiceNotInstalled = errors.New("Service is not installed")
)
