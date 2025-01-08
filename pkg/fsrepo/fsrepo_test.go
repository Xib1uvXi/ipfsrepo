package fsrepo

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/require"
	"os"
	"path/filepath"
	"testing"

	datastore "github.com/ipfs/go-datastore"
)

func TestInitIdempotence(t *testing.T) {
	t.Parallel()
	path := t.TempDir()
	for i := 0; i < 10; i++ {
		require.Nil(t, Init(path), "multiple calls to init should succeed")
	}
}

func Remove(repoPath string) error {
	repoPath = filepath.Clean(repoPath)
	return os.RemoveAll(repoPath)
}

func TestCanManageReposIndependently(t *testing.T) {
	t.Parallel()
	pathA := t.TempDir()
	pathB := t.TempDir()

	t.Log("initialize two repos")
	require.Nil(t, Init(pathA), "a", "should initialize successfully")
	require.Nil(t, Init(pathB), "b", "should initialize successfully")

	t.Log("open the two repos")
	repoA, err := open(pathA)
	require.Nil(t, err, "a")
	repoB, err := open(pathB)
	require.Nil(t, err, "b")

	t.Log("close and remove b while a is open")
	require.Nil(t, repoB.Close(), "close b")
	require.Nil(t, Remove(pathB), "remove b")

	t.Log("close and remove a")
	require.Nil(t, repoA.Close())
	require.Nil(t, Remove(pathA))
}

func TestDatastoreGetNotAllowedAfterClose(t *testing.T) {
	t.Parallel()
	path := t.TempDir()

	require.Nil(t, Init(path), "should initialize successfully")
	r, err := open(path)
	require.Nil(t, err, "should open successfully")

	k := "key"
	data := []byte(k)
	require.Nil(t, r.Datastore().Put(context.Background(), datastore.NewKey(k), data), "Put should be successful")

	require.Nil(t, r.Close())
	_, err = r.Datastore().Get(context.Background(), datastore.NewKey(k))
	require.Error(t, err, "after closer, Get should be fail")
}

func TestDatastorePersistsFromRepoToRepo(t *testing.T) {
	t.Parallel()
	path := t.TempDir()

	require.Nil(t, Init(path))
	r1, err := open(path)
	require.Nil(t, err)

	k := "key"
	expected := []byte(k)
	require.Nil(t, r1.Datastore().Put(context.Background(), datastore.NewKey(k), expected), "using first repo, Put should be successful")
	require.Nil(t, r1.Close())

	r2, err := open(path)
	require.Nil(t, err)
	actual, err := r2.Datastore().Get(context.Background(), datastore.NewKey(k))
	require.Nil(t, err, "using second repo, Get should be successful")
	require.Nil(t, r2.Close())
	require.True(t, bytes.Equal(expected, actual), "data should match")
}
