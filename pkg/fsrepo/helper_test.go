package fsrepo

import (
	"github.com/stretchr/testify/require"
	"os"
	"testing"
)

func TestDatastoreSpec(t *testing.T) {
	tmpPath := t.TempDir()
	result := DatastoreSpec(tmpPath)
	require.Equal(t, tmpPath+"/datastore_spec", result)
}

func TestFileExists(t *testing.T) {
	tmpPath := t.TempDir()

	// Test file not exists
	result := FileExists(tmpPath + "/not_exists")
	require.False(t, result)

	// create file
	f, err := os.Create(tmpPath + "/exists")
	require.NoError(t, err)
	f.Close()

	// Test file exists
	result = FileExists(tmpPath + "/exists")
	require.True(t, result)
}

func TestWritable(t *testing.T) {
	tmpPath := t.TempDir()

	// Test writable
	err := Writable(tmpPath)
	require.NoError(t, err)

	// Test not writable
	err = os.Chmod(tmpPath, 0)
	require.NoError(t, err)
	err = Writable(tmpPath)
	require.Error(t, err)
}
