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

package timestamp

import (
	"time"

	s "github.com/beevik/ntp"
)

type ntp struct {
	r *s.Response
}

func NewNtp() (TsGenerator, error) {
	r, err := s.Query("0.beevik-ntp.pool.ntp.org")
	if err != nil {
		return nil, err
	}
	return &ntp{r}, nil
}

func (w *ntp) Now() time.Time {
	return time.Now().Add(w.r.ClockOffset)
}
