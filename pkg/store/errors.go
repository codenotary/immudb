/*
Copyright 2019-2020 vChain, Inc.

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

package store

import (
	"github.com/dgraph-io/badger/v2"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// immudb errors
var (
	ErrInconsistentState = status.New(codes.Unknown, "inconsistent state").Err()
	ErrIndexNotFound     = status.New(codes.NotFound, "index not found").Err()
	ErrInvalidKey        = status.New(codes.InvalidArgument, "invalid key").Err()
	ErrInvalidReference  = status.New(codes.InvalidArgument, "invalid reference").Err()
	ErrInvalidKeyPrefix  = status.New(codes.InvalidArgument, "invalid key prefix").Err()
	ErrInvalidOffset     = status.New(codes.InvalidArgument, "invalid offset").Err()
	ErrInvalidRootIndex  = status.New(codes.InvalidArgument, "invalid root index").Err()
)

// fixme(leogr): review codes and fix/remove errors which do not make sense in this context, finally correct comments accordingly.
var (
	// ErrValueLogSize is returned when opt.ValueLogFileSize option is not within the valid
	// range.
	ErrValueLogSize = status.New(codes.Unknown, badger.ErrValueLogSize.Error()).Err()

	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = status.New(codes.NotFound, badger.ErrKeyNotFound.Error()).Err()

	// ErrTxnTooBig is returned if too many writes are fit into a single transaction.
	ErrTxnTooBig = status.New(codes.Unknown, badger.ErrTxnTooBig.Error()).Err()

	// ErrConflict is returned when a transaction conflicts with another transaction. This can
	// happen if the read rows had been updated concurrently by another transaction.
	ErrConflict = status.New(codes.Unknown, badger.ErrConflict.Error()).Err()

	// ErrReadOnlyTxn is returned if an update function is called on a read-only transaction.
	ErrReadOnlyTxn = status.New(codes.Unknown, badger.ErrReadOnlyTxn.Error()).Err()

	// ErrDiscardedTxn is returned if a previously discarded transaction is re-used.
	ErrDiscardedTxn = status.New(codes.Unknown, badger.ErrDiscardedTxn.Error()).Err()

	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = status.New(codes.Unknown, badger.ErrEmptyKey.Error()).Err()

	// note(leogr): special case, we reuse the immudb's ErrInvalidKey
	// // ErrInvalidKey is returned if the key has a special !badger! prefix,
	// // reserved for internal usage.
	// ErrInvalidKey = status.New(codes.Unknown, badger.// ErrInvalidKey.Error()).Err() "Key is using a reserved !badger! prefix")

	// ErrRetry is returned when a log file containing the value is not found.
	// This usually indicates that it may have been garbage collected, and the
	// operation needs to be retried.
	ErrRetry = status.New(codes.Unknown, badger.ErrRetry.Error()).Err()

	// ErrThresholdZero is returned if threshold is set to zero, and value log GC is called.
	// In such a case, GC can't be run.
	ErrThresholdZero = status.New(codes.Unknown, badger.ErrThresholdZero.Error()).Err()

	// ErrNoRewrite is returned if a call for value log GC doesn't result in a log file rewrite.
	ErrNoRewrite = status.New(codes.Unknown, badger.ErrNoRewrite.Error()).Err()

	// ErrRejected is returned if a value log GC is called either while another GC is running, or
	// after DB::Close has been called.
	ErrRejected = status.New(codes.Unknown, badger.ErrRejected.Error()).Err()

	// ErrInvalidRequest is returned if the user request is invalid.
	ErrInvalidRequest = status.New(codes.Unknown, badger.ErrInvalidRequest.Error()).Err()

	// ErrManagedTxn is returned if the user tries to use an API which isn't
	// allowed due to external management of transactions, when using ManagedDB.
	ErrManagedTxn = status.New(codes.Unknown, badger.ErrManagedTxn.Error()).Err()

	// ErrInvalidDump if a data dump made previously cannot be loaded into the database.
	ErrInvalidDump = status.New(codes.Unknown, badger.ErrInvalidDump.Error()).Err()

	// ErrZeroBandwidth is returned if the user passes in zero bandwidth for sequence.
	ErrZeroBandwidth = status.New(codes.Unknown, badger.ErrZeroBandwidth.Error()).Err()

	// ErrInvalidLoadingMode is returned when opt.ValueLogLoadingMode option is not
	// within the valid range
	ErrInvalidLoadingMode = status.New(codes.Unknown, badger.ErrInvalidLoadingMode.Error()).Err()

	// ErrReplayNeeded is returned when opt.ReadOnly is set but the
	// database requires a value log replay.
	ErrReplayNeeded = status.New(codes.Unknown, badger.ErrReplayNeeded.Error()).Err()

	// ErrWindowsNotSupported is returned when opt.ReadOnly is used on Windows
	ErrWindowsNotSupported = status.New(codes.Unknown, badger.ErrWindowsNotSupported.Error()).Err()

	// ErrTruncateNeeded is returned when the value log gets corrupt, and requires truncation of
	// corrupt data to allow Badger to run properly.
	ErrTruncateNeeded = status.New(codes.Unknown, badger.ErrTruncateNeeded.Error()).Err()

	// ErrBlockedWrites is returned if the user called DropAll. During the process of dropping all
	// data from Badger, we stop accepting new writes, by returning this error.
	ErrBlockedWrites = status.New(codes.Unknown, badger.ErrBlockedWrites.Error()).Err()

	// ErrNilCallback is returned when subscriber's callback is nil.
	ErrNilCallback = status.New(codes.Unknown, badger.ErrNilCallback.Error()).Err()

	// ErrNoPrefixes is returned when subscriber doesn't provide any prefix.
	ErrNoPrefixes = status.New(codes.Unknown, badger.ErrNoPrefixes.Error()).Err()

	// ErrEncryptionKeyMismatch is returned when the storage key is not
	// matched with the key previously given.
	ErrEncryptionKeyMismatch = status.New(codes.Unknown, badger.ErrEncryptionKeyMismatch.Error()).Err()

	// ErrInvalidDataKeyID is returned if the datakey id is invalid.
	ErrInvalidDataKeyID = status.New(codes.Unknown, badger.ErrInvalidDataKeyID.Error()).Err()

	ErrInvalidEncryptionKey = status.New(codes.Unknown, badger.ErrInvalidEncryptionKey.Error()).Err()

	ErrGCInMemoryMode = status.New(codes.Unknown, badger.ErrGCInMemoryMode.Error()).Err()
)

var badgerErrorsMap = map[error]error{
	badger.ErrValueLogSize:          ErrValueLogSize,
	badger.ErrKeyNotFound:           ErrKeyNotFound,
	badger.ErrTxnTooBig:             ErrTxnTooBig,
	badger.ErrConflict:              ErrConflict,
	badger.ErrReadOnlyTxn:           ErrReadOnlyTxn,
	badger.ErrDiscardedTxn:          ErrDiscardedTxn,
	badger.ErrEmptyKey:              ErrEmptyKey,
	badger.ErrInvalidKey:            ErrInvalidKey,
	badger.ErrRetry:                 ErrRetry,
	badger.ErrThresholdZero:         ErrThresholdZero,
	badger.ErrNoRewrite:             ErrNoRewrite,
	badger.ErrRejected:              ErrRejected,
	badger.ErrInvalidRequest:        ErrInvalidRequest,
	badger.ErrManagedTxn:            ErrManagedTxn,
	badger.ErrInvalidDump:           ErrInvalidDump,
	badger.ErrZeroBandwidth:         ErrZeroBandwidth,
	badger.ErrInvalidLoadingMode:    ErrInvalidLoadingMode,
	badger.ErrReplayNeeded:          ErrReplayNeeded,
	badger.ErrWindowsNotSupported:   ErrWindowsNotSupported,
	badger.ErrTruncateNeeded:        ErrTruncateNeeded,
	badger.ErrBlockedWrites:         ErrBlockedWrites,
	badger.ErrNilCallback:           ErrNilCallback,
	badger.ErrNoPrefixes:            ErrNoPrefixes,
	badger.ErrEncryptionKeyMismatch: ErrEncryptionKeyMismatch,
	badger.ErrInvalidDataKeyID:      ErrInvalidDataKeyID,
	badger.ErrInvalidEncryptionKey:  ErrInvalidEncryptionKey,
	badger.ErrGCInMemoryMode:        ErrGCInMemoryMode,
}

// mapError maps an underlying error (usually coming from badger) to an immudb's error.
// mapError MUST NOT be used on errors defined within this package.
func mapError(err error) error {
	if err == nil {
		return nil
	}

	// Is err a badger's error?
	if e, ok := badgerErrorsMap[err]; ok {
		return e
	}

	// otherwise just wrap it as unknown
	return status.Newf(codes.Unknown, "unknown error: %s", err.Error()).Err()
}
