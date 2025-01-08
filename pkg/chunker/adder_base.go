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

type Result struct {
	FileName      string
	FileSizeBytes int64
	FileHumanSize string
	ChunkSize     string
	RootCid       string
	Blocks        []string
}

type AdderBase struct {
	ctx    context.Context
	cancel context.CancelFunc
	*adder
}

func NewAdderBase(pctx context.Context, dagService ipld.DAGService, chunkSize int64) *AdderBase {
	ctx, cancel := context.WithCancel(pctx)
	return &AdderBase{adder: newAdder(ctx, dagService, chunkSize), ctx: ctx, cancel: cancel}
}

func (s *AdderBase) Add(targetPath string) (*Result, error) {
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
		ChunkSize:     humanize.Bytes(uint64(s.chunkSize)),
		RootCid:       nd.Cid().String(),
		Blocks:        links,
	}, nil
}
