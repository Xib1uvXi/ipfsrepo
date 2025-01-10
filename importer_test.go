package ipfsrepo

import (
	"context"
	"github.com/Xib1uvXi/ipfsrepo/pkg/chunker"
	"github.com/Xib1uvXi/ipfsrepo/pkg/fsrepo"
	"github.com/ipfs/boxo/blockstore"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"testing"
)

func TestNewImporter(t *testing.T) {
	tmpRoot := t.TempDir()
	repo, err := fsrepo.NewFSRepo(tmpRoot)
	require.NoError(t, err)
	defer repo.Close()

	bs := blockstore.NewBlockstore(repo.Datastore(), blockstore.WriteThrough(true))

	i := NewImporter(bs, chunker.Chunk1MiB)
	require.False(t, i.running.Load())

	tmpDir := t.TempDir()

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

	result, err := i.Import(context.Background(), tmpPath)
	require.NoError(t, err)

	require.NotEmpty(t, result.RootCid)
	t.Logf("root cid: %s", result.RootCid)
}

func TestNewImporter_RealWorld(t *testing.T) {
	t.Skip("local test")
	tmpRoot := t.TempDir()
	repo, err := fsrepo.NewFSRepo(tmpRoot)
	require.NoError(t, err)
	defer repo.Close()

	bs := blockstore.NewBlockstore(repo.Datastore(), blockstore.WriteThrough(true))

	i := NewImporter(bs, chunker.Chunk1MiB)

	result, err := i.Import(context.Background(), "~/Downloads/chunkdata/chunk_testdata_4G.tar.gz")
	require.NoError(t, err)

	t.Logf("filename: %s", result.FileName)
	t.Logf("rootCid: %s", result.RootCid)
	t.Logf("chunk size: %v", result.ChunkSize)
	t.Logf("size: %v", result.FileSizeBytes)
	t.Logf("size: %v", result.FileHumanSize)
	t.Logf("blocks: %v", len(result.Blocks))
}
