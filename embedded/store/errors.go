/*
Copyright 2025 Codenotary Inc. All rights reserved.

SPDX-License-Identifier: BUSL-1.1
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    https://mariadb.com/bsl11/

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package store

import (
	"errors"
	"fmt"

	"github.com/codenotary/immudb/v2/embedded"
	"github.com/codenotary/immudb/v2/embedded/tbtree"
)

var (
	ErrIllegalArguments                    = embedded.ErrIllegalArguments
	ErrInvalidOptions                      = fmt.Errorf("%w: invalid options", ErrIllegalArguments)
	ErrAlreadyClosed                       = embedded.ErrAlreadyClosed
	ErrUnexpectedLinkingError              = errors.New("internal inconsistency between linear and binary linking")
	ErrNoEntriesProvided                   = errors.New("no entries provided")
	ErrWriteOnlyTx                         = errors.New("write-only transaction")
	ErrReadOnlyTx                          = errors.New("read-only transaction")
	ErrTxReadConflict                      = errors.New("tx read conflict")
	ErrTxAlreadyCommitted                  = errors.New("tx already committed")
	ErrMaxTxEntriesLimitExceeded           = errors.New("max number of entries per tx exceeded")
	ErrNullKey                             = errors.New("null key")
	ErrMaxKeyLenExceeded                   = errors.New("max key length exceeded")
	ErrMaxValueLenExceeded                 = errors.New("max value length exceeded")
	ErrPreconditionFailed                  = errors.New("precondition failed")
	ErrDuplicatedKey                       = errors.New("duplicated key")
	ErrCannotUpdateKeyTransiency           = errors.New("cannot change a non-transient key to transient or vice versa")
	ErrMaxActiveTransactionsLimitExceeded  = errors.New("max active transactions limit exceeded")
	ErrMVCCReadSetLimitExceeded            = errors.New("MVCC read-set limit exceeded")
	ErrMaxConcurrencyLimitExceeded         = errors.New("max concurrency limit exceeded")
	ErrMaxIndexersLimitExceeded            = errors.New("max indexers limit exceeded")
	ErrPathIsNotADirectory                 = errors.New("path is not a directory")
	ErrCorruptedTxData                     = errors.New("tx data is corrupted")
	ErrCorruptedTxDataMaxTxEntriesExceeded = fmt.Errorf("%w: maximum number of TX entries exceeded", ErrCorruptedTxData)
	ErrTxEntryIndexOutOfRange              = errors.New("tx entry index out of range")
	ErrCorruptedTxDataUnknownHeaderVersion = fmt.Errorf("%w: unknown TX header version", ErrCorruptedTxData)
	ErrCorruptedTxDataMaxKeyLenExceeded    = fmt.Errorf("%w: maximum key length exceeded", ErrCorruptedTxData)
	ErrCorruptedTxDataDuplicateKey         = fmt.Errorf("%w: duplicate key in a single TX", ErrCorruptedTxData)
	ErrCorruptedData                       = errors.New("data is corrupted")
	ErrCorruptedCLog                       = errors.New("commit-log is corrupted")
	ErrCorruptedIndex                      = errors.New("corrupted index")
	ErrTxSizeGreaterThanMaxTxSize          = errors.New("tx size greater than max tx size")
	ErrCorruptedAHtree                     = errors.New("appendable hash tree is corrupted")
	ErrKeyNotFound                         = tbtree.ErrKeyNotFound // TODO: define error in store layer
	ErrExpiredEntry                        = fmt.Errorf("%w: expired entry", ErrKeyNotFound)
	ErrKeyAlreadyExists                    = errors.New("key already exists")
	ErrTxNotFound                          = errors.New("tx not found")
	ErrNoMoreEntries                       = tbtree.ErrNoMoreEntries    // TODO: define error in store layer
	ErrIllegalState                        = tbtree.ErrIllegalState     // TODO: define error in store layer
	ErrOffsetOutOfRange                    = tbtree.ErrOffsetOutOfRange // TODO: define error in store layer
	ErrUnexpectedError                     = errors.New("unexpected error")
	ErrUnsupportedTxVersion                = errors.New("unsupported tx version")
	ErrNewerVersionOrCorruptedData         = errors.New("tx created with a newer version or data is corrupted")
	ErrTxPoolExhausted                     = errors.New("transaction pool exhausted")

	ErrInvalidPrecondition                  = errors.New("invalid precondition")
	ErrInvalidPreconditionTooMany           = fmt.Errorf("%w: too many preconditions", ErrInvalidPrecondition)
	ErrInvalidPreconditionNull              = fmt.Errorf("%w: null", ErrInvalidPrecondition)
	ErrInvalidPreconditionNullKey           = fmt.Errorf("%w: %v", ErrInvalidPrecondition, ErrNullKey)
	ErrInvalidPreconditionMaxKeyLenExceeded = fmt.Errorf("%w: %v", ErrInvalidPrecondition, ErrMaxKeyLenExceeded)
	ErrInvalidPreconditionInvalidTxID       = fmt.Errorf("%w: invalid transaction ID", ErrInvalidPrecondition)

	ErrSourceTxNewerThanTargetTx = fmt.Errorf("%w: source tx is newer than target tx", ErrIllegalArguments)

	ErrCompactionDisabled = errors.New("compaction is disabled")

	ErrMetadataUnsupported = errors.New(
		"metadata is unsupported when in 1.1 compatibility mode, " +
			"do not use metadata-related features such as expiration and logical deletion",
	)

	ErrUnsupportedTxHeaderVersion         = errors.New("missing tx header serialization method")
	ErrIllegalTruncationArgument          = fmt.Errorf("%w: invalid truncation info", ErrIllegalArguments)
	ErrTruncationInfoNotPresentInMetadata = errors.New("truncation info not present in metadata")

	ErrInvalidProof = errors.New("invalid proof")

	ErrIndexNotFound           = errors.New("index not found")
	ErrIndexAlreadyInitialized = errors.New("index already initialized")
)
