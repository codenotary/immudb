package schema

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrUnexpectedNotStructuredValue = status.New(codes.FailedPrecondition, "unexpected not structured value encountered").Err()
	ErrEmptySet                     = status.New(codes.InvalidArgument, "empty set").Err()
	ErrDuplicateKeysNotSupported    = status.New(codes.InvalidArgument, "duplicate keys are not supported in single batch transaction").Err()
)
