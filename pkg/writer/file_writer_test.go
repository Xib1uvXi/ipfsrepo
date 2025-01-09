package writer

import (
	"context"
	"crypto/sha256"
	"github.com/Xib1uvXi/ipfsrepo/pkg/chunker"
	"github.com/Xib1uvXi/ipfsrepo/pkg/fsrepo"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/sync"
	"github.com/mitchellh/go-homedir"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"io"
	"os"
	"path"
	gofilepath "path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"
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
	t.Skip("local test")
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

func TestNewSrv3_RealWorld_4G_1MiB(t *testing.T) {
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
	ab := chunker.NewAdderBase(ctx, dsrv, chunker.Chunk1MiB)

	result, err := opzRun(ab, "~/Downloads/chunkdata/chunk_testdata_4G.tar.gz", t)
	require.NoError(t, err)

	rawHash, err := sha256File("~/Downloads/chunkdata/chunk_testdata_4G.tar.gz")
	require.NoError(t, err)

	srv := NewSrv(dsrv)

	toPath := "/tmp/output/" + result.FileName

	require.NoError(t, srv.WriteTo(ctx, result.RootCid, toPath))

	hash, err := sha256File(toPath)
	require.NoError(t, err)

	assert.Equal(t, rawHash, hash)
}

func TestNewSrv4_RealWorld_49G_1MiB(t *testing.T) {
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
	ab := chunker.NewAdderBase(ctx, dsrv, chunker.Chunk1MiB)

	result, err := opzRun(ab, "~/Downloads/chunkdata/chunk_testdata_49G.tar.gz", t)
	require.NoError(t, err)

	rawHash, err := sha256File("~/Downloads/chunkdata/chunk_testdata_49G.tar.gz")
	require.NoError(t, err)

	srv := NewSrv(dsrv)

	toPath := "/tmp/output/" + result.FileName

	require.NoError(t, srv.WriteTo(ctx, result.RootCid, toPath))

	hash, err := sha256File(toPath)
	require.NoError(t, err)

	assert.Equal(t, rawHash, hash)
}

func TestNewSrv5_RealWorld_49G_10MiB(t *testing.T) {
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
	ab := chunker.NewAdderBase(ctx, dsrv, chunker.Chunk10MiB)

	result, err := opzRun(ab, "~/Downloads/chunkdata/chunk_testdata_49G.tar.gz", t)
	require.NoError(t, err)

	rawHash, err := sha256File("~/Downloads/chunkdata/chunk_testdata_49G.tar.gz")
	require.NoError(t, err)

	srv := NewSrv(dsrv)

	toPath := "/tmp/output/" + result.FileName

	require.NoError(t, srv.WriteTo(ctx, result.RootCid, toPath))

	hash, err := sha256File(toPath)
	require.NoError(t, err)

	assert.Equal(t, rawHash, hash)
}

func opzRun(adder *chunker.AdderBase, path string, t *testing.T) (*chunker.Result, error) {
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

func sha256File(filePath string) (string, error) {
	expPath, err := homedir.Expand(gofilepath.Clean(filePath))
	if err != nil {
		return "", err
	}

	// Open the file
	file, err := os.Open(expPath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Create a new SHA256 hash object
	hasher := sha256.New()

	// Copy the file contents into the hasher
	if _, err := io.Copy(hasher, file); err != nil {
		return "", err
	}

	// Get the final hash sum
	hash := hasher.Sum(nil)

	return string(hash), nil
}
