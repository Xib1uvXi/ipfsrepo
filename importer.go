package ipfsrepo

import (
	"context"
	"errors"
	"github.com/Xib1uvXi/ipfsrepo/pkg/chunker"
	"github.com/ipfs/boxo/blockservice"
	"github.com/ipfs/boxo/blockstore"
	"github.com/ipfs/boxo/exchange/offline"
	"github.com/ipfs/boxo/ipld/merkledag"
	"go.uber.org/atomic"
	"path/filepath"
	"time"
)

var (
	ErrImporterAlreadyRunning = errors.New("importer is already running")
)

type ImportProgressInfo struct {
	PathName string
	Progress float64
}

func (p *ImportProgressInfo) Update(progress float64) {
	p.Progress = progress
}

type Importer struct {
	chunkSize    int64
	blockStore   blockstore.Blockstore
	ProgressInfo *ImportProgressInfo
	running      *atomic.Bool
}

func NewImporter(blockStore blockstore.Blockstore, chunkSize int64) *Importer {
	return &Importer{
		blockStore:   blockStore,
		running:      atomic.NewBool(false),
		ProgressInfo: &ImportProgressInfo{},
		chunkSize:    chunkSize,
	}
}

func (i *Importer) Import(ctx context.Context, path string) (*chunker.Result, error) {
	defer func() {
		i.ProgressInfo = &ImportProgressInfo{}
	}()

	if !i.running.CompareAndSwap(false, true) {
		return nil, ErrImporterAlreadyRunning
	}

	defer i.running.Store(false)

	bsrv := blockservice.New(i.blockStore, offline.Exchange(i.blockStore))
	dsrv := merkledag.NewDAGService(bsrv)

	ab, clean := chunker.NewAdderWithBar(ctx, dsrv, i.chunkSize)
	defer clean()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	i.ProgressInfo.PathName = filepath.Base(path)

	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				i.ProgressInfo.Update(ab.Progress())
			}
		}
	}()

	result, err := ab.Add(path)
	if err != nil {
		return nil, err
	}

	return result, nil
}

// Progress returns the current progress of the import
func (i *Importer) Progress() *ImportProgressInfo {
	return i.ProgressInfo
}
