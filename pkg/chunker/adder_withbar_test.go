package chunker

import (
	"context"
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
	"testing"
	"time"
)

func TestNewAdderWithBar(t *testing.T) {
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
	ab, clean := NewAdderWithBar(ctx, dsrv, Chunk1MiB)
	defer clean()

	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.Logf("%f", ab.Progress())
			}
		}
	}()

	result, err := ab.Add(testFilePath)
	require.NoError(t, err)

	require.Equal(t, 1, len(result.Blocks))
	require.Equal(t, "bafkreidc6b4nw5nrlvpghs76xxwin34tpxpjqmht44gbu72a3ndtv4u72m", result.RootCid)
}

func TestNewAdderWithBar2(t *testing.T) {
	t.Skip("local test")
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	ctx := context.Background()
	ab, clean := NewAdderWithBar(ctx, dsrv, Chunk1MiB)
	defer clean()

	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.Logf("%f", ab.Progress())
			}
		}
	}()

	result, err := ab.Add("~/Downloads/testdata/tt2")
	require.NoError(t, err)

	t.Logf("filename: %s", result.FileName)
	t.Logf("rootCid: %s", result.RootCid)
	t.Logf("size: %v", result.FileSizeBytes)
	t.Logf("size: %v", result.FileHumanSize)
	t.Logf("blocks: %v", len(result.Blocks))
}

func TestNewAdderWithBar3(t *testing.T) {
	t.Skip("local test")
	ds := sync.MutexWrap(datastore.NewMapDatastore())
	bs := blockstore.NewBlockstore(ds)
	bsrv := blockservice.New(bs, offline.Exchange(bs))
	dsrv := merkledag.NewDAGService(bsrv)

	ctx := context.Background()
	ab, clean := NewAdderWithBar(ctx, dsrv, Chunk1MiB)
	defer clean()

	go func() {
		ticker := time.NewTicker(time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				t.Logf("%f", ab.Progress())
			}
		}
	}()

	result, err := ab.Add("~/Downloads/chunkdata/chunk_testdata_4G.tar.gz")
	require.NoError(t, err)

	t.Logf("filename: %s", result.FileName)
	t.Logf("rootCid: %s", result.RootCid)
	t.Logf("chunk size: %v", result.ChunkSize)
	t.Logf("size: %v", result.FileSizeBytes)
	t.Logf("size: %v", result.FileHumanSize)
	t.Logf("blocks: %v", len(result.Blocks))
}
