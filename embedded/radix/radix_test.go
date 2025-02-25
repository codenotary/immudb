package radix

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTreeInsert(t *testing.T) {
	t.Run("insert monotonic sequence", func(t *testing.T) {
		tree := New()

		var key [4]byte
		var value [4]byte

		n := 1000
		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint32(key[:], uint32(i))
			binary.BigEndian.PutUint32(value[:], uint32(i+1))

			err := tree.Insert(key[:], value[:], 0)
			require.NoError(t, err)
		}

		for i := 0; i < n; i++ {
			binary.BigEndian.PutUint32(key[:], uint32(i))
			binary.BigEndian.PutUint32(value[:], uint32(i+1))

			v, _, err := tree.Get(key[:])
			require.NoError(t, err)
			require.Equal(t, value[:], v)
		}

		binary.BigEndian.PutUint32(key[:], uint32(n))
		_, _, err := tree.Get(key[:])
		require.ErrorIs(t, err, ErrKeyNotFound)
	})

	t.Run("test edge cases", func(t *testing.T) {
		tree := New()
		err := tree.Insert(nil, []byte{1}, 0)
		require.ErrorIs(t, err, ErrEmptyKey)

		err = tree.Insert([]byte("test"), []byte{}, 0)
		require.NoError(t, err)

		err = tree.Insert([]byte("test"), []byte{}, 0)
		require.ErrorIs(t, err, ErrDuplicateKey)

		err = tree.Insert(make([]byte, MaxKeySize+1), []byte{1}, 0)
		require.ErrorIs(t, err, ErrMaxKeyLenExceeded)
	})
}

func TestTreeIterator(t *testing.T) {
	tree := New()

	var key [4]byte
	var value [4]byte

	n := 1000
	for i := 0; i < n; i += 2 {
		binary.BigEndian.PutUint32(key[:], uint32(i))
		binary.BigEndian.PutUint32(value[:], uint32(i+1))

		err := tree.Insert(key[:], value[:], 0)
		require.NoError(t, err)
	}

	t.Run("seek", func(t *testing.T) {
		t.Run("seek first", func(t *testing.T) {
			it := tree.NewIterator(false)
			it.Seek(nil)

			e, err := it.Next()
			require.NoError(t, err)

			binary.BigEndian.PutUint32(key[:], uint32(0))
			require.Equal(t, key[:], e.Key)
		})

		t.Run("seek last", func(t *testing.T) {
			binary.BigEndian.PutUint32(key[:], uint32(n))

			it := tree.NewIterator(true)
			it.Seek(key[:])

			e, err := it.Next()
			require.NoError(t, err)

			binary.BigEndian.PutUint32(key[:], uint32(n-2))
			require.Equal(t, key[:], e.Key)
		})

		t.Run("seek none", func(t *testing.T) {
			it := tree.NewIterator(true)
			it.Seek(nil)

			_, err := it.Next()
			require.ErrorIs(t, err, ErrStopIteration)

			binary.BigEndian.PutUint32(key[:], uint32(n))

			it = tree.NewIterator(false)
			it.Seek(key[:])

			_, err = it.Next()
			require.ErrorIs(t, err, ErrStopIteration)
		})

		t.Run("seek to non existing key", func(t *testing.T) {
			seekKey := 2*uint32(rand.Intn(n/2)) + 1
			binary.BigEndian.PutUint32(key[:], uint32(seekKey))

			it := tree.NewIterator(false)
			it.Seek(key[:])

			seekKey++

			for {
				e, err := it.Next()
				if errors.Is(err, ErrStopIteration) {
					break
				}
				require.NoError(t, err)

				binary.BigEndian.PutUint32(key[:], uint32(seekKey))
				require.Equal(t, key[:], e.Key)

				seekKey += 2
			}
			require.Equal(t, n, int(seekKey))
		})
	})

	t.Run("forward iteration", func(t *testing.T) {
		seekKey := 2 * uint32(rand.Intn(n/2+1))
		binary.BigEndian.PutUint32(key[:], uint32(seekKey))

		it := tree.NewIterator(false)
		it.Seek(key[:])

		for {
			e, err := it.Next()
			if errors.Is(err, ErrStopIteration) {
				break
			}
			require.NoError(t, err)

			binary.BigEndian.PutUint32(key[:], uint32(seekKey))
			require.Equal(t, key[:], e.Key)
			seekKey += 2
		}
		require.Equal(t, n, int(seekKey))
	})

	t.Run("backward iteration", func(t *testing.T) {
		seekKey := 2 * uint32(rand.Intn(n/2))
		binary.BigEndian.PutUint32(key[:], uint32(seekKey))

		it := tree.NewIterator(true)
		it.Seek(key[:])

		for {
			e, err := it.Next()
			if errors.Is(err, ErrStopIteration) {
				break
			}
			require.NoError(t, err)

			binary.BigEndian.PutUint32(key[:], uint32(seekKey))
			require.Equal(t, key[:], e.Key)
			seekKey -= 2
		}

		var expectedSeekKey uint32
		expectedSeekKey -= 2
		require.Equal(t, expectedSeekKey, seekKey)
	})
}

func TestTreeInsertRetrieve(t *testing.T) {
	type testCase struct {
		name         string
		count        int
		maxLength    int
		prefixChance float64
	}

	cases := []testCase{
		{"Minimal Case", 5, 3, 0.0},
		{"All Unique", 10, 5, 0.0},
		{"All Prefixes", 10, 5, 1.0},
		{"Single-Character Words", 50, 1, 0.5},
		{"Long Words, Low Count", 10, 50, 0.7},
		{"Very Long Words", 5, 500, 0.5},
		{"High Collision Prefixes", 20, 10, 0.9},
		{"Completely Random", 50, 10, 0.5},
		{"Massive Input", 10000, 20, 0.7},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			words := genRandomWords(tc.count, tc.maxLength, tc.prefixChance)
			n := len(words)

			tree := New()
			for _, w := range words {
				err := tree.Insert([]byte(w), []byte{}, 0)
				require.NoError(t, err)
			}

			for _, w := range words {
				_, _, err := tree.Get([]byte(w))
				require.NoError(t, err)
			}

			sort.Slice(words, func(i, j int) bool {
				return bytes.Compare([]byte(words[i]), []byte(words[j])) < 0
			})

			t.Run("forward iteration", func(t *testing.T) {
				m := 0
				it := tree.NewIterator(false)
				it.Seek(nil)

				for {
					e, err := it.Next()
					if errors.Is(err, ErrStopIteration) {
						break
					}

					require.NoError(t, err)
					require.Equal(t, words[m], string(e.Key))
					m++
				}
				require.Equal(t, n, m)
			})

			t.Run("backward iteration", func(t *testing.T) {
				it := tree.NewIterator(true)

				s := make([]byte, tc.maxLength+1)
				for i := range s {
					s[i] = 255
				}
				it.Seek(s)

				m := 0
				for {
					e, err := it.Next()
					if errors.Is(err, ErrStopIteration) {
						break
					}

					require.NoError(t, err)
					require.Equal(t, words[len(words)-m-1], string(e.Key))
					m++
				}
				require.Equal(t, n, m)
			})
		})
	}
}

func genRandomWords(
	count int,
	maxLength int,
	prefixChance float64,
) []string {
	wm := make(map[string]bool)

	words := make([]string, count)
	words[0] = randWord(rand.Intn(maxLength) + 1)
	wm[words[0]] = true

	runs := 0
	for len(wm) < count && runs < count*10 {
		runs++

		n := len(wm)
		if rand.Float64() < prefixChance {
			base := words[rand.Intn(len(words))]
			if len(base) < maxLength {
				extraLength := rand.Intn(maxLength-len(base)) + 1
				w := base + randWord(extraLength)
				if !wm[w] {
					words[n] = w
					wm[w] = true
				}
			}
		} else {
			w := randWord(rand.Intn(maxLength) + 1)
			if !wm[w] {
				words[n] = w
				wm[w] = true
			}
		}
	}
	return words[:len(wm)]
}

func randWord(n int) string {
	var sb strings.Builder
	for i := 0; i < n; i++ {
		sb.WriteByte(byte(rand.Intn(MaxChildren)))
	}
	return sb.String()
}
