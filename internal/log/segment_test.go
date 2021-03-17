package log

import (
	"io"
	"io/ioutil"
	"os"
	"testing"

	api "github.com/cdarne/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestSegmentMaxIndex(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}
	c := Config{}
	c.Segment.MaxStoreBytes = 1024
	c.Segment.MaxIndexBytes = entryWidth * 3
	var baseOffset uint64 = 16

	s, err := newSegment(dir, baseOffset, c)
	require.NoError(t, err)
	require.Equal(t, baseOffset, s.nextOffset)
	require.False(t, s.IsMaxed())

	for i := 0; i < 3; i++ {
		off, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, baseOffset+uint64(i), off)
		require.Equal(t, baseOffset+uint64(i), want.Offset)

		got, err := s.Read(off)
		require.NoError(t, err)
		require.Equal(t, off, got.Offset)
		require.Equal(t, want.Value, got.Value)
	}
	_, err = s.Append(want)
	require.Equal(t, io.EOF, err)
	// Maxed Index
	require.True(t, s.IsMaxed())
}

func TestSegmentMaxStore(t *testing.T) {
	dir, _ := ioutil.TempDir("", "segment-test")
	defer os.RemoveAll(dir)

	want := &api.Record{Value: []byte("hello world")}
	c := Config{}
	c.Segment.MaxStoreBytes = uint64(len(want.Value))
	c.Segment.MaxIndexBytes = 1024
	var baseOffset uint64 = 16

	s, err := newSegment(dir, baseOffset, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())

	off, err := s.Append(want)
	require.NoError(t, err)
	require.Equal(t, baseOffset, off)
	require.Equal(t, baseOffset, want.Offset)

	// Maxed store
	require.True(t, s.IsMaxed())

	err = s.Remove()
	require.NoError(t, err)

	s, err = newSegment(dir, baseOffset, c)
	require.NoError(t, err)
	require.False(t, s.IsMaxed())
}
