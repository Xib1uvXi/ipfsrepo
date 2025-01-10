package ipfsrepo

import (
	"context"
	"github.com/dustin/go-humanize"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestNewStorageUsage(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	maxStorage := uint64(1024)

	usage, err := NewStorageUsage(ctx, tmpDir, maxStorage)
	require.NoError(t, err)
	require.NotNil(t, usage)
	usage.SetThreshold(70.0)
	usage.SetScanInterval(5 * time.Millisecond)
	usage.Start()

	require.False(t, usage.IsFull())
	require.Equal(t, 5*time.Millisecond, usage.scanInterval)
	require.Equal(t, 70.0, usage.threshold)
	require.Equal(t, humanize.Bytes(maxStorage), usage.MaxStorage())

	time.Sleep(10 * time.Millisecond)

	tmpPath := tmpDir + "/input"
	require.NoError(t, os.MkdirAll(tmpPath+"/testdir", 0755))
	require.NoError(t, os.MkdirAll(tmpPath+"/testdir2", 0755))

	fileBytes, err := createFile0to100k()
	require.NoError(t, err)
	// write file to disk
	testFilePath := path.Join(tmpPath+"/testdir", "testfile1")
	assert.NoError(t, os.WriteFile(testFilePath, fileBytes, 0644))

	fileBytes, err = createFile0to200k()
	require.NoError(t, err)
	// write file to disk
	testFilePath = path.Join(tmpPath+"/testdir", "testfile2")
	assert.NoError(t, os.WriteFile(testFilePath, fileBytes, 0644))

	time.Sleep(10 * time.Millisecond)

	t.Logf("usage: %s", humanize.Bytes(usage.usage))
	t.Logf("Usage: %f", usage.UsagePercentage())

	require.True(t, usage.IsFull())
}

// createFile0to100k creates a file with the number 0 to 100k
func createFile0to100k() ([]byte, error) {
	b := strings.Builder{}
	for i := 0; i <= 100000; i++ {
		s := strconv.Itoa(i)
		_, err := b.WriteString(s)
		if err != nil {
			return nil, err
		}
	}
	return []byte(b.String()), nil
}

// createFile0to200k creates a file with the number 0 to 200k
func createFile0to200k() ([]byte, error) {
	b := strings.Builder{}
	for i := 0; i <= 200000; i++ {
		s := strconv.Itoa(i)
		_, err := b.WriteString(s)
		if err != nil {
			return nil, err
		}
	}
	return []byte(b.String()), nil
}

func TestStorageUsage_getStorageUsage(t *testing.T) {
	t.Skip("local test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	tmpDir := t.TempDir()
	maxStorage := uint64(1024)

	usage, err := NewStorageUsage(ctx, tmpDir, maxStorage)
	require.NoError(t, err)
	require.NotNil(t, usage)
	usage.SetThreshold(70.0)
	usage.SetScanInterval(5 * time.Millisecond)
	usage.Start()

	size, err := usage.getStorageUsage("/Users/xib/Downloads/chunkdata")
	require.NoError(t, err)

	t.Logf("Size: %s", humanize.Bytes(size))
}
