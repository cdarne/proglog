package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	write = []byte("hello world")
	width = lenWidth + uint64(len(write))
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func TestClose(t *testing.T) {
	f, err := ioutil.TempFile("", "store_close_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())
	s, err := newStore(f)
	require.NoError(t, err)
	_, _, err = s.Append(write)
	require.NoError(t, err)

	f, beforeSize, err := openFile(f.Name())
	require.NoError(t, err)

	err = s.Close()
	require.NoError(t, err)

	_, afterSize, err := openFile(f.Name())
	require.NoError(t, err)
	require.True(t, afterSize > beforeSize)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(write)
		require.NoError(t, err)
		require.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, write, read)
		pos += width
	}
}

func testReadAt(t *testing.T, s *store) {
	t.Helper()
	for i, offset := uint64(1), int64(0); i < 4; i++ {
		buffer := make([]byte, lenWidth)
		n, err := s.ReadAt(buffer, offset)
		require.NoError(t, err)
		require.Equal(t, lenWidth, n)
		offset += int64(n)

		size := enc.Uint64(buffer)
		buffer = make([]byte, size)
		n, err = s.ReadAt(buffer, offset)
		require.NoError(t, err)
		require.Equal(t, write, buffer)
		require.Equal(t, int(size), n)
		offset += int64(n)
	}
}

func openFile(name string) (file *os.File, size int64, err error) {
	// f, err := os.OpenFile(name, os.O_RDONLY|os.O_CREATE|os.O_APPEND, 0644)
	f, err := os.OpenFile(name, os.O_RDONLY, 0444)
	if err != nil {
		return nil, 0, err
	}
	fileInfo, err := f.Stat()
	if err != nil {
		return nil, 0, err
	}
	return f, fileInfo.Size(), nil
}
