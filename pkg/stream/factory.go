/*
Copyright 2024 Codenotary Inc. All rights reserved.

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

package stream

type serviceFactory struct {
	ChunkSize int
}

// ServiceFactory returns high level immudb streaming services
// High level services are capable to receive and send immudb transportation objects. Those services rely on internal more generic receiver and sender services.
type ServiceFactory interface {
	NewMsgReceiver(str ImmuServiceReceiver_Stream) MsgReceiver
	NewMsgSender(str ImmuServiceSender_Stream) MsgSender

	NewKvStreamReceiver(str MsgReceiver) KvStreamReceiver
	NewKvStreamSender(str MsgSender) KvStreamSender

	NewVEntryStreamReceiver(str MsgReceiver) VEntryStreamReceiver
	NewVEntryStreamSender(str MsgSender) VEntryStreamSender

	NewZStreamReceiver(str MsgReceiver) ZStreamReceiver
	NewZStreamSender(str MsgSender) ZStreamSender

	NewExecAllStreamSender(str MsgSender) ExecAllStreamSender
	NewExecAllStreamReceiver(str MsgReceiver) ExecAllStreamReceiver
}

// NewStreamServiceFactory returns a new ServiceFactory
func NewStreamServiceFactory(chunkSize int) ServiceFactory {
	return &serviceFactory{ChunkSize: chunkSize}
}

// NewMsgSender returns a MsgSender
func (s *serviceFactory) NewMsgSender(str ImmuServiceSender_Stream) MsgSender {
	return NewMsgSender(str, make([]byte, s.ChunkSize))
}

// NewMsgReceiver returns a MsgReceiver
func (s *serviceFactory) NewMsgReceiver(str ImmuServiceReceiver_Stream) MsgReceiver {
	return NewMsgReceiver(str)
}

// NewKvStreamReceiver returns a KvStreamReceiver
func (s *serviceFactory) NewKvStreamReceiver(mr MsgReceiver) KvStreamReceiver {
	return NewKvStreamReceiver(mr, s.ChunkSize)
}

// NewKvStreamSender returns a KvStreamSender
func (s *serviceFactory) NewKvStreamSender(ms MsgSender) KvStreamSender {
	return NewKvStreamSender(ms)
}

func (s *serviceFactory) NewVEntryStreamReceiver(mr MsgReceiver) VEntryStreamReceiver {
	return NewVEntryStreamReceiver(mr, s.ChunkSize)
}

func (s *serviceFactory) NewVEntryStreamSender(ms MsgSender) VEntryStreamSender {
	return NewVEntryStreamSender(ms)
}

// NewZStreamReceiver returns a ZStreamReceiver
func (s *serviceFactory) NewZStreamReceiver(mr MsgReceiver) ZStreamReceiver {
	return NewZStreamReceiver(mr, s.ChunkSize)
}

// NewZStreamSender returns a ZStreamSender
func (s *serviceFactory) NewZStreamSender(ms MsgSender) ZStreamSender {
	return NewZStreamSender(ms)
}

// NewExecAllStreamReceiver returns a ExecAllStreamReceiver
func (s *serviceFactory) NewExecAllStreamReceiver(mr MsgReceiver) ExecAllStreamReceiver {
	return NewExecAllStreamReceiver(mr, s.ChunkSize)
}

// NewExecAllStreamSender returns a ExecAllStreamSender
func (s *serviceFactory) NewExecAllStreamSender(ms MsgSender) ExecAllStreamSender {
	return NewExecAllStreamSender(ms)
}
