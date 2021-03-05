package client

import (
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/codenotary/immudb/pkg/stream"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImmuServer_StreamVerifiedSetAndGet(t *testing.T) {
	skipTestIfNoImmudbServer(t)

	cliIF, ctx := newImmuClient(t)

	fileNames := []string{
		"/Users/ogg/Downloads/tmpogg/learn/migrating-to-microservice-databases.pdf",
		"/Users/ogg/Downloads/Pneuma_by_Tool_LIVE.mp4",
	}

	kvs := make([]*stream.KeyValue, len(fileNames))
	vSizes := make([]int, len(fileNames))
	for i, fileName := range fileNames {
		f, kv, err := inputTestFileToStreamKV(t, fileName)
		require.NoError(t, err)
		defer f.Close()
		kvs[i] = kv
		vSizes[i] = kv.Value.Size
	}

	txMeta, err := cliIF.StreamVerifiedSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, txMeta)

	for i, fileName := range fileNames {
		entry, err := cliIF.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{
			KeyRequest: &schema.KeyRequest{Key: []byte(fileName)},
		})
		assert.NoError(t, err)
		assert.Equal(t, vSizes[i]+1, len(entry.Value))
	}
}
