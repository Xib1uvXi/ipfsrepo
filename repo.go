package ipfsrepo

import (
	"context"
	"github.com/Xib1uvXi/ipfsrepo/pkg/chunker"
	"github.com/Xib1uvXi/ipfsrepo/pkg/fsrepo"
	"github.com/Xib1uvXi/ipfsrepo/pkg/linuxutils/lsblk"
	"github.com/Xib1uvXi/ipfsrepo/pkg/writer"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"time"
)

type RepoOption func(*Repo) error

func SetBlockStoreWithCache(cache blockstore.CacheOpts, blockOpts ...blockstore.Option) RepoOption {
	return func(r *Repo) error {
		blockStore := blockstore.NewBlockstore(r.storage.Datastore(), blockOpts...)
		blockStore, err := blockstore.CachedBlockstore(r.ctx, blockStore, cache)
		if err != nil {
			return err
		}

		r.blockStore = blockStore

		return nil
	}
}

func SetBlockStore(blockOpts ...blockstore.Option) RepoOption {
	return func(r *Repo) error {
		r.blockStore = blockstore.NewBlockstore(r.storage.Datastore(), blockOpts...)
		return nil
	}
}

func SetChunkSize(chunkSize int64) RepoOption {
	return func(r *Repo) error {
		r.importer = NewImporter(r.blockStore, chunkSize)
		return nil
	}
}

func SetStorageUsage(scanInterval time.Duration, threshold float64) RepoOption {
	return func(r *Repo) error {
		r.StorageUsage.SetScanInterval(scanInterval)
		r.StorageUsage.SetThreshold(threshold)
		return nil
	}

}

type Repo struct {
	ctx         context.Context
	cancel      context.CancelFunc
	blockDevice *lsblk.BlockDevice
	storage     fsrepo.Storage
	blockStore  blockstore.Blockstore
	importer    *Importer
	*StorageUsage
	*BlockRepo
}

// FromPath creates a new repo from the given path
func FromPath(uuid string, repoPath string, maxStorage uint64, opts ...RepoOption) (*Repo, error) {
	storage, err := fsrepo.NewFSRepo(repoPath)
	if err != nil {
		return nil, err
	}
	mockBlockDevice := &lsblk.BlockDevice{Size: maxStorage, UUID: uuid}
	ctx, cancel := context.WithCancel(context.Background())

	storageUsage, err := NewStorageUsage(ctx, repoPath, maxStorage)
	if err != nil {
		cancel()
		return nil, err
	}

	r := &Repo{ctx: ctx, cancel: cancel, storage: storage, blockDevice: mockBlockDevice, StorageUsage: storageUsage}

	for _, opt := range opts {
		if err := opt(r); err != nil {
			return nil, err
		}
	}

	if r.blockStore == nil {
		r.blockStore = blockstore.NewBlockstore(storage.Datastore(), blockstore.WriteThrough(true))
	}

	if r.importer == nil {
		r.importer = NewImporter(r.blockStore, chunker.Chunk1MiB)
	}

	r.StorageUsage.Start()
	r.BlockRepo = &BlockRepo{blockStore: r.blockStore}

	return r, nil
}

// UUID returns UUID of the block device
func (r *Repo) UUID() string {
	return r.blockDevice.UUID
}

// MaxStorageSize returns the maximum size of the block device
func (r *Repo) MaxStorageSize() uint64 {
	return r.blockDevice.Size
}

// Close closes the repo
func (r *Repo) Close() {
	r.cancel()

	if r.storage != nil {
		_ = r.storage.Close()
	}
}

// Extract the block from the repo, writes it to the given path
func (r *Repo) Extract(ctx context.Context, rootCid string, toPath string) error {
	bSrv := blockservice.New(r.blockStore, offline.Exchange(r.blockStore))
	dSrv := merkledag.NewDAGService(bSrv)

	return writer.NewSrv(dSrv).WriteTo(ctx, rootCid, toPath)
}

// Import the file to the repo
func (r *Repo) Import(ctx context.Context, path string) (*chunker.Result, error) {
	return r.importer.Import(ctx, path)
}

// ImportProgressInfo returns the progress info of the importer
func (r *Repo) ImportProgressInfo() *ImportProgressInfo {
	return r.importer.ProgressInfo
}
