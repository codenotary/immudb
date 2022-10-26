package ahtree

import (
	"crypto/sha256"
	"math"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAnalysis(t *testing.T) {

	const totalNodes = 100000
	const finalNodesToCheck = 100

	tree, err := Open("./analysis", DefaultOptions())
	require.NoError(t, err)

	t.Run("prepare data", func(t *testing.T) {
		for tree.Size() < totalNodes {
			_, _, err := tree.Append([]byte{byte(tree.Size())})
			require.NoError(t, err)

			if tree.Size()%1000 == 0 {
				t.Logf("Tree size: %d", tree.Size())
			}

			if tree.Size()%10000 == 0 {
				err = tree.Sync()
				require.NoError(t, err)
			}
		}
		err = tree.Sync()
		require.NoError(t, err)
	})

	t.Logf("Tree size: %v", tree.Size())
	size := tree.Size()

	t.Run("analyze inclusion proofs", func(t *testing.T) {
		nodeAtOffsetBuffer = nodeAtOffsetBuffer[0:0]

		for i := size - finalNodesToCheck + 1; i <= size; i++ {
			for j := i; j <= size; j++ {
				_, err := tree.InclusionProof(i, j)
				require.NoError(t, err)
			}
		}

		minOffset := uint64(math.MaxUint64)
		t.Logf("Offset buffer size: %v", len(nodeAtOffsetBuffer))

		t.Logf("dLogSize (entries): %v", tree.dLogSize/sha256.Size)

		for _, offset := range nodeAtOffsetBuffer {
			if offset < minOffset {
				minOffset = offset
			}
		}
		t.Logf("Min offset: %v", minOffset)
	})

	t.Run("analyze consistency proofs", func(t *testing.T) {
		nodeAtOffsetBuffer = nodeAtOffsetBuffer[0:0]

		for i := size - finalNodesToCheck + 1; i <= size; i++ {
			for j := i; j <= size; j++ {
				_, err := tree.ConsistencyProof(i, j)
				require.NoError(t, err)
			}
		}

		minOffset := uint64(math.MaxUint64)
		t.Logf("Offset buffer size: %v", len(nodeAtOffsetBuffer))

		t.Logf("dLogSize (entries): %v", tree.dLogSize/sha256.Size)

		for _, offset := range nodeAtOffsetBuffer {
			if offset < minOffset {
				minOffset = offset
			}
		}
		t.Logf("Min offset: %v", minOffset)
	})

}
