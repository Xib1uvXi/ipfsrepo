package chunker

import (
	"context"
	"errors"
	"github.com/dustin/go-humanize"
	"github.com/ipfs/boxo/ipld/merkledag"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/mitchellh/go-homedir"
	gofilepath "path/filepath"
)

type AdderWithBar struct {
	*AdderBase
	out      chan interface{}
	total    int64
	addSize  int64
	swapSize int64
}

func NewAdderWithBar(pctx context.Context, dagService ipld.DAGService, chunkSize int64) (*AdderWithBar, func()) {
	out := make(chan interface{}, 128)
	a := NewAdderBase(pctx, dagService, chunkSize)
	adderS := &AdderWithBar{AdderBase: a, out: out, total: 0, addSize: 0, swapSize: 0}

	go adderS.handleOut(a.ctx)
	a.adder.Out = out

	return adderS, a.cancel
}

func (s *AdderWithBar) Add(targetPath string) (*Result, error) {
	expPath, err := homedir.Expand(gofilepath.Clean(targetPath))
	if err != nil {
		return nil, err
	}

	wrapFilePath, err := NewFilePath(expPath)
	if err != nil {
		return nil, err
	}

	addit := wrapFilePath.Entries()
	if !addit.Next() {
		return nil, errors.New("no files found")
	}

	fsize, err := addit.Node().Size()
	if err != nil {
		return nil, err
	}

	s.total += fsize

	hSize := humanize.Bytes(uint64(fsize))
	filename := gofilepath.Base(addit.Name())

	nd, err := s.adder.SetBaseName(filename).Add(addit.Node())
	if err != nil {
		return nil, err
	}

	visited := cid.NewSet()
	err = merkledag.Walk(s.ctx, merkledag.GetLinksWithDAG(s.dagService), nd.Cid(), func(c cid.Cid) bool {
		if !visited.Visit(c) {
			return false
		}
		return true
	}, merkledag.Concurrent())

	if err != nil {
		return nil, err
	}

	var links []string = make([]string, 0, visited.Len())
	err = visited.ForEach(func(c cid.Cid) error {
		links = append(links, c.String())
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &Result{
		FileName:      filename,
		FileSizeBytes: fsize,
		FileHumanSize: hSize,
		ChunkSize:     GetChunkSize(int(s.chunkSize)),
		RootCid:       nd.Cid().String(),
		Blocks:        links,
	}, nil
}

func (s *AdderWithBar) Progress() float64 {
	if s.total == 0 {
		return 0
	}

	return float64(s.addSize) / float64(s.total) * 100
}

func (s *AdderWithBar) handleOut(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case result := <-s.out:
			event, ok := result.(*AddEvent)
			if !ok {
				continue
			}

			if event.Bytes == event.Size {
				s.swapSize += event.Size
				s.addSize = s.swapSize
				continue
			}

			s.addSize = event.Bytes + s.swapSize
		}
	}
}
