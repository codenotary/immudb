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

package stream

import (
	"github.com/codenotary/immudb/pkg/api/schema"
)

// ImmuServiceSender_Stream is used to inject schema.ImmuService_StreamGetServer, schema.ImmuService_StreamZScanServer inside both client and server senders
type ImmuServiceSender_Stream interface {
	Send(*schema.Chunk) error
	RecvMsg(m interface{}) error // used to retrieve server side errors
}

// ImmuServiceReceiver_Stream is used to inject schema.ImmuService_StreamGetClient, schema.ImmuService_StreamGetClient, schema.ImmuService_StreamHistoryClient and similar inside both client and server receivers
type ImmuServiceReceiver_Stream interface {
	Recv() (*schema.Chunk, error)
}
