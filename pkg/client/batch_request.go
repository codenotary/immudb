package client

import (
	"io"
	"io/ioutil"

	"github.com/codenotary/immudb/pkg/schema"
)

type BatchRequest struct {
	Keys   []io.Reader
	Values []io.Reader
}

func (b *BatchRequest) toBatchSetRequest() (*schema.BatchSetRequest, error) {
	var setRequests []*schema.SetRequest
	for i, _ := range b.Keys {
		key, err := ioutil.ReadAll(b.Keys[i])
		if err != nil {
			return nil, err
		}
		value, err := ioutil.ReadAll(b.Values[i])
		if err != nil {
			return nil, err
		}
		setRequests = append(setRequests, &schema.SetRequest{
			Key:   key,
			Value: value,
		})
	}
	return &schema.BatchSetRequest{SetRequests: setRequests}, nil
}
