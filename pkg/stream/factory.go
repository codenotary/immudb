/*
Copyright 2021 CodeNotary, Inc. All rights reserved.

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

type serviceFactory struct {
	ChunkSize int
}

// serviceFactory returns high level immudb streaming services
// High level services are capable to receive and send immudb transportation objects. Those services rely on internal more generic receiver and sender services.
type ServiceFactory interface {
	NewKvStreamReceiver(str ImmuServiceReceiver_Stream) KvStreamReceiver
	NewKvStreamSender(str ImmuServiceSender_Stream) KvStreamSender

	NewVEntryStreamReceiver(str ImmuServiceReceiver_Stream) VEntryStreamReceiver
	NewVEntryStreamSender(str ImmuServiceSender_Stream) VEntryStreamSender

	NewZStreamReceiver(str ImmuServiceReceiver_Stream) ZStreamReceiver
	NewZStreamSender(str ImmuServiceSender_Stream) ZStreamSender
}

// NewStreamServiceFactory returns a new ServiceFactory
func NewStreamServiceFactory(chunkSize int) ServiceFactory {
	return &serviceFactory{ChunkSize: chunkSize}
}

// NewKvStreamReceiver returns a KvStreamReceiver
func (s *serviceFactory) NewKvStreamReceiver(str ImmuServiceReceiver_Stream) KvStreamReceiver {
	return NewKvStreamReceiver(NewMsgReceiver(str), s.ChunkSize)
}

// NewKvStreamSender returns a KvStreamSender
func (s *serviceFactory) NewKvStreamSender(str ImmuServiceSender_Stream) KvStreamSender {
	return NewKvStreamSender(NewMsgSender(str, s.ChunkSize))
}

func (s *serviceFactory) NewVEntryStreamReceiver(str ImmuServiceReceiver_Stream) VEntryStreamReceiver {
	return NewVEntryStreamReceiver(NewMsgReceiver(str), s.ChunkSize)
}

func (s *serviceFactory) NewVEntryStreamSender(str ImmuServiceSender_Stream) VEntryStreamSender {
	return NewVEntryStreamSender(NewMsgSender(str, s.ChunkSize))
}

// NewZStreamReceiver returns a ZStreamReceiver
func (s *serviceFactory) NewZStreamReceiver(str ImmuServiceReceiver_Stream) ZStreamReceiver {
	return NewZStreamReceiver(NewMsgReceiver(str), s.ChunkSize)
}

// NewZStreamSender returns a ZStreamSender
func (s *serviceFactory) NewZStreamSender(str ImmuServiceSender_Stream) ZStreamSender {
	return NewZStreamSender(NewMsgSender(str, s.ChunkSize))
}
