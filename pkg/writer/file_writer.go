package writer

import (
	"context"
	"github.com/ipfs/boxo/files"
	unixfile "github.com/ipfs/boxo/ipld/unixfs/file"
	cid2 "github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
)

type Srv struct {
	dagSrv ipld.DAGService
}

func NewSrv(dagSrv ipld.DAGService) *Srv {
	return &Srv{
		dagSrv: dagSrv,
	}
}

func (s *Srv) WriteTo(ctx context.Context, rootCid string, toPath string) error {
	cid, err := cid2.Parse(rootCid)
	if err != nil {
		return err
	}

	node, err := s.dagSrv.Get(ctx, cid)
	if err != nil {
		return err
	}

	fileNode, err := unixfile.NewUnixfsFile(ctx, s.dagSrv, node)
	if err != nil {
		return err
	}

	return files.WriteTo(fileNode, toPath)
}
