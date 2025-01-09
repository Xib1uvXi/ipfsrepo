package chunker

import (
	"context"
	"github.com/Xib1uvXi/ipfsrepo/pkg/fsrepo"
	"github.com/dustin/go-humanize"
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
	"time"
)

// createFile0to100k creates a file with the number 0 to 100k
// bafkreidc6b4nw5nrlvpghs76xxwin34tpxpjqmht44gbu72a3ndtv4u72m 1 for 1MiB chunk
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

func TestNewAdderBase(t *testing.T) {
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)

	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	tmpPath := t.TempDir()

	fileBytes, err := createFile0to100k()
	require.NoError(t, err)

	// write file to disk
	testFilePath := path.Join(tmpPath, "testfile")
	assert.NoError(t, os.WriteFile(testFilePath, fileBytes, 0644))

	ctx := context.Background()
	ab := NewAdderBase(ctx, dsrv, Chunk1MiB)

	result, err := ab.Add(testFilePath)
	require.NoError(t, err)

	require.Equal(t, 1, len(result.Blocks))
	require.Equal(t, "bafkreidc6b4nw5nrlvpghs76xxwin34tpxpjqmht44gbu72a3ndtv4u72m", result.RootCid)

	for _, r := range result.Blocks {
		t.Logf("block cid: %s", r)
	}

	t.Logf("rootCid: %s", result.RootCid)
	t.Logf("blocks: %v", result.Blocks)
}

func TestNewAdderBase2(t *testing.T) {
	t.Skip("local test")
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	ctx := context.Background()
	ab := NewAdderBase(ctx, dsrv, Chunk1MiB)

	result, err := ab.Add("~/Downloads/testdata/tt2")
	require.NoError(t, err)

	t.Logf("filename: %s", result.FileName)
	t.Logf("rootCid: %s", result.RootCid)
	t.Logf("size: %v", result.FileSizeBytes)
	t.Logf("size: %v", result.FileHumanSize)
	t.Logf("blocks: %v", len(result.Blocks))
}

func TestNewAdderBase3(t *testing.T) {
	t.Skip("local test")
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	ctx := context.Background()
	ab := NewAdderBase(ctx, dsrv, Chunk1MiB)

	result, err := ab.Add("~/Downloads/chunkdata/chunk_testdata_4G.tar.gz")
	require.NoError(t, err)

	t.Logf("filename: %s", result.FileName)
	t.Logf("rootCid: %s", result.RootCid)
	t.Logf("chunk size: %v", result.ChunkSize)
	t.Logf("size: %v", result.FileSizeBytes)
	t.Logf("size: %v", result.FileHumanSize)
	t.Logf("blocks: %v", len(result.Blocks))
}

func TestNewAdderBase4_RealWorld_4G_1MiB(t *testing.T) {
	t.Skip("local test")
	tmpRoot := t.TempDir()
	repo, err := fsrepo.NewFSRepo(tmpRoot)
	require.NoError(t, err)
	defer repo.Close()

	bs := blockstore.NewBlockstore(repo.Datastore(), blockstore.WriteThrough(true))
	//bs := blockstore.NewBlockstore(repo.Datastore())
	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	ctx := context.Background()
	ab := NewAdderBase(ctx, dsrv, Chunk1MiB)

	_, err = opzRun(ab, "~/Downloads/chunkdata/chunk_testdata_4G.tar.gz", t)
	require.NoError(t, err)
}

func TestNewAdderBase4_RealWorld_49G_1MiB(t *testing.T) {
	t.Skip("local test")
	tmpRoot := t.TempDir()
	repo, err := fsrepo.NewFSRepo(tmpRoot)
	require.NoError(t, err)
	defer repo.Close()

	bs := blockstore.NewBlockstore(repo.Datastore(), blockstore.WriteThrough(true))
	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	ctx := context.Background()
	ab := NewAdderBase(ctx, dsrv, Chunk1MiB)

	_, err = opzRun(ab, "~/Downloads/chunkdata/chunk_testdata_49G.tar.gz", t)
	require.NoError(t, err)
}

func TestNewAdderBase4_RealWorld_49G_10MiB(t *testing.T) {
	t.Skip("local test")
	tmpRoot := t.TempDir()
	repo, err := fsrepo.NewFSRepo(tmpRoot)
	require.NoError(t, err)
	defer repo.Close()

	bs := blockstore.NewBlockstore(repo.Datastore(), blockstore.WriteThrough(true))
	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	ctx := context.Background()
	ab := NewAdderBase(ctx, dsrv, Chunk10MiB)

	_, err = opzRun(ab, "~/Downloads/chunkdata/chunk_testdata_49G.tar.gz", t)
	require.NoError(t, err)
}

func opzRun(adder *AdderBase, path string, t *testing.T) (*Result, error) {
	now := time.Now()

	result, err := adder.Add(path)
	if err != nil {
		return nil, err
	}

	spend := time.Since(now).Seconds()
	speed := float64(result.FileSizeBytes) / spend

	t.Logf("perf: %v/s", humanize.Bytes(uint64(speed)))

	t.Logf("filename: %s", result.FileName)
	t.Logf("rootCid: %s", result.RootCid)
	t.Logf("chunk size: %v", result.ChunkSize)
	t.Logf("size: %v", result.FileSizeBytes)
	t.Logf("size: %v", result.FileHumanSize)
	t.Logf("blocks: %v", len(result.Blocks))

	return result, nil
}
