package client

import (
	"github.com/codenotary/immudb/pkg/stream/streamtest"
	"github.com/codenotary/immudb/pkg/streamutils"
	"os"
	"testing"

	"github.com/codenotary/immudb/pkg/api/schema"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestImmuServer_StreamVerifiedSetAndGet(t *testing.T) {
	skipTestIfNoImmudbServer(t)

	cliIF, ctx := newImmuClient(t)

	tmpFile, err := streamtest.GenerateDummyFile("myFile1", (8<<20)-1)
	require.NoError(t, err)
	defer tmpFile.Close()
	defer os.Remove(tmpFile.Name())

	kvs, err := streamutils.GetKeyValuesFromFiles(tmpFile.Name())
	if err != nil {
		t.Error(err)
	}

	txMeta, err := cliIF.StreamVerifiedSet(ctx, kvs)
	require.NoError(t, err)
	require.NotNil(t, txMeta)

	entry, err := cliIF.StreamVerifiedGet(ctx, &schema.VerifiableGetRequest{
		KeyRequest: &schema.KeyRequest{Key: []byte(tmpFile.Name())},
	})
	assert.NoError(t, err)
	assert.Equal(t, (8<<20)-1, len(entry.Value))
}
