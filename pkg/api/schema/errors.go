package schema

import (
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	ErrUnexpectedNotStructuredValue     = status.New(codes.FailedPrecondition, "unexpected not structured value encountered").Err()
	ErrEmptySet                         = status.New(codes.InvalidArgument, "empty set").Err()
	ErrDuplicatedKeysNotSupported       = status.New(codes.InvalidArgument, "duplicated keys are not supported in single batch transaction").Err()
	ErrDuplicatedZAddNotSupported       = status.New(codes.InvalidArgument, "duplicated index inside zAdd insertions are not supported in single batch transaction").Err()
	ErrDuplicatedReferencesNotSupported = status.New(codes.InvalidArgument, "duplicated references insertions are not supported in single batch transaction").Err()
)
