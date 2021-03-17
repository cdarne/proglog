package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	f, err := ioutil.TempFile(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	c := Config{}
	c.Segment.MaxIndexBytes = 1024
	idx, err := newIndex(f, c)
	require.NoError(t, err)

	// Reading an empty index file
	_, _, err = idx.Read(-1)
	require.Error(t, err)
	require.Equal(t, f.Name(), idx.Name())

	entries := []struct {
		Offset   uint32
		Position uint64
	}{
		{Offset: 0, Position: 0},
		{Offset: 1, Position: 10},
		{Offset: 2, Position: 20},
	}

	for _, want := range entries {
		err = idx.Write(want.Offset, want.Position)
		require.NoError(t, err)

		_, position, err := idx.Read(int64(want.Offset))
		require.NoError(t, err)
		require.Equal(t, want.Position, position)
	}

	// error while reading past existing entries
	_, _, err = idx.Read(int64(len(entries)))
	require.Equal(t, io.EOF, err)
	_ = idx.Close()

	// index should build its state from the existing file
	f, _ = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	idx, err = newIndex(f, c)
	require.NoError(t, err)
	// Reading the last offset/position
	offset, position, err := idx.Read(-1)
	require.NoError(t, err)
	require.Equal(t, uint32(2), offset)
	require.Equal(t, uint64(20), position)
}
