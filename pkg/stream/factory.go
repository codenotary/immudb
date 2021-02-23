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

import "github.com/codenotary/immudb/pkg/api/schema"

type serviceFactory struct{}

type ServiceFactory interface {
	NewKvStreamReceiver(str schema.ImmuService_StreamSetServer) KvStreamReceiver
	NewKvStreamSender(str schema.ImmuService_StreamGetServer) KvStreamSender
}

func NewStreamServiceFactory() ServiceFactory {
	return &serviceFactory{}
}

func (s *serviceFactory) NewKvStreamReceiver(str schema.ImmuService_StreamSetServer) KvStreamReceiver {
	return NewKvStreamReceiver(NewMsgReceiver(str))
}

func (s *serviceFactory) NewKvStreamSender(str schema.ImmuService_StreamGetServer) KvStreamSender {
	return NewKvStreamSender(NewMsgSender(str))
}
