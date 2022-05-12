/*
Copyright 2022 CodeNotary, Inc. All rights reserved.

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
package remoteapp

import "errors"

var (
	ErrIllegalArguments        = errors.New("illegal arguments")
	ErrMissingRemoteChunk      = errors.New("missing data on the remote storage")
	ErrInvalidLocalStorage     = errors.New("invalid local storage")
	ErrInvalidRemoteStorage    = errors.New("invalid remote storage")
	ErrInvalidChunkState       = errors.New("invalid chunk state")
	ErrChunkUploaded           = errors.New("already uploaded chunk is not writable")
	ErrCompressionNotSupported = errors.New("compression is currently not supported")
	ErrCantDownload            = errors.New("can not download chunk")
	ErrCorruptedMetadata       = errors.New("corrupted metadata in a remote chunk")
)
