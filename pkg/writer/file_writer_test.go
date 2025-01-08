package writer

import (
	"context"
	"github.com/Xib1uvXi/ipfsrepo/pkg/chunker"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"os"
	"path"
	"strconv"
	"strings"
	"testing"
)

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

func TestNewSrv(t *testing.T) {
	tmpPath := t.TempDir() + "/input"
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

	ds := sync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	ctx := context.Background()
	ab := chunker.NewAdderBase(ctx, dsrv, chunker.Chunk1MiB)
	result, err := ab.Add(tmpPath)
	require.NoError(t, err)
	require.True(t, len(result.Blocks) > 0)

	srv := NewSrv(dsrv)

	respPath := t.TempDir() + "/output/"
	require.NoError(t, srv.WriteTo(ctx, result.RootCid, respPath))

	// 判断文件夹路径是否存在
	check1 := path.Join(respPath, "testdir")
	check2 := path.Join(respPath, "testdir2")
	_, err = os.Stat(check1)
	require.NoError(t, err)

	_, err = os.Stat(check2)
	require.NoError(t, err)

	// 判断文件是否存在
	check3 := path.Join(respPath, "testdir", "testfile1")
	check4 := path.Join(respPath, "testdir", "testfile2")

	_, err = os.Stat(check3)
	require.NoError(t, err)

	_, err = os.Stat(check4)
	require.NoError(t, err)
}

func TestNewSrv2(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	ctx := context.Background()
	ab := chunker.NewAdderBase(ctx, dsrv, chunker.Chunk1MiB)

	result, err := ab.Add("~/Downloads/chunkdata/chunk_testdata_4G.tar.gz")
	require.NoError(t, err)

	t.Logf("filename: %s", result.FileName)
	t.Logf("rootCid: %s", result.RootCid)
	t.Logf("chunk size: %v", result.ChunkSize)
	t.Logf("size: %v", result.FileSizeBytes)
	t.Logf("size: %v", result.FileHumanSize)
	t.Logf("blocks: %v", len(result.Blocks))

	srv := NewSrv(dsrv)

	toPath := "/tmp/output/" + result.FileName

	require.NoError(t, srv.WriteTo(ctx, result.RootCid, toPath))
}
