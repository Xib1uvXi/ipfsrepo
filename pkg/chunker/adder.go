package chunker

import (
	"context"
	"errors"
	"fmt"
	"github.com/ipfs/boxo/chunker"
	"github.com/ipfs/boxo/files"
	"github.com/ipfs/boxo/ipld/merkledag"
	dagtest "github.com/ipfs/boxo/ipld/merkledag/test"
	"github.com/ipfs/boxo/ipld/unixfs"
	"github.com/ipfs/boxo/ipld/unixfs/importer/balanced"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
	"github.com/ipfs/boxo/mfs"
	"github.com/ipfs/go-cid"
	ipld "github.com/ipfs/go-ipld-format"
	"github.com/multiformats/go-multicodec"
	"io"
	"os"
	"path"
)

func NewFilePath(path string) (files.Directory, error) {
	stat, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}

	var file files.Node

	if stat.IsDir() {
		file, err = files.NewSerialFile(path, false, stat)
		if err != nil {
			return nil, err
		}
	} else {
		readfile, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		file = files.NewReaderStatFile(readfile, stat)
	}

	return files.NewSliceDirectory([]files.DirEntry{files.FileEntry(path, file)}), nil
}

type AdderOpt func(*adder)

func EnableProgressBar(out chan<- interface{}) AdderOpt {
	return func(a *adder) {
		a.Out = out
	}
}

type adder struct {
	ctx        context.Context
	chunkSize  int64
	dagService ipld.DAGService
	bufferedDS *ipld.BufferedDAG
	cidBuilder cid.Builder
	mroot      *mfs.Root
	liveNodes  uint64
	baseName   string

	Out chan<- interface{}
}

func (a *adder) SetBaseName(baseName string) *adder {
	a.baseName = baseName
	return a
}

func newAdder(ctx context.Context, dagService ipld.DAGService, chunkSize int64, opts ...AdderOpt) *adder {
	bufferedDS := ipld.NewBufferedDAG(ctx, dagService, ipld.MaxSizeBatchOption(100<<20), ipld.MaxNodesBatchOption(128))

	a := &adder{ctx: ctx, dagService: dagService, bufferedDS: bufferedDS}
	a.cidBuilder = cid.V1Builder{ // Use CIDv1 for all links
		Codec:    uint64(multicodec.DagPb),
		MhType:   uint64(multicodec.Sha2_256), // Use SHA2-256 as the hash function
		MhLength: -1,                          // Use the default hash length for the given hash function (in this case 256 bits)
	}

	a.chunkSize = chunkSize

	for _, opt := range opts {
		opt(a)
	}

	return a
}

func (a *adder) Add(file files.Node) (ipld.Node, error) {
	if err := a.addFileNode(a.ctx, "", file, true); err != nil {
		return nil, err
	}

	// get root
	mr, err := a.mfsRoot()
	if err != nil {
		return nil, err
	}
	var root mfs.FSNode
	rootdir := mr.GetDirectory()
	root = rootdir

	err = root.Flush()
	if err != nil {
		return nil, err
	}

	// if adding a file without wrapping, swap the root to it (when adding a
	// directory, mfs root is the directory)
	_, dir := file.(files.Directory)
	var name string
	if !dir {
		children, err := rootdir.ListNames(a.ctx)
		if err != nil {
			return nil, err
		}

		if len(children) == 0 {
			return nil, fmt.Errorf("expected at least one child dir, got none")
		}

		// Replace root with the first child
		name = children[0]
		root, err = rootdir.Child(name)
		if err != nil {
			return nil, err
		}
	}

	err = mr.Close()
	if err != nil {
		return nil, err
	}

	nd, err := root.GetNode()
	if err != nil {
		return nil, err
	}

	return nd, nil
}

// SetTestMfsRoot SetMfsRoot sets `r` as the root for adder.
func (a *adder) SetTestMfsRoot() {
	md := dagtest.Mock()
	emptyDirNode := unixfs.EmptyDirNode()

	if err := emptyDirNode.SetCidBuilder(a.cidBuilder); err != nil {
		panic(err)
	}

	mr, err := mfs.NewRoot(context.Background(), md, emptyDirNode, nil)
	if err != nil {
		panic(err)
	}

	a.mroot = mr
}

func (a *adder) mfsRoot() (*mfs.Root, error) {
	if a.mroot != nil {
		return a.mroot, nil
	}
	rnode := unixfs.EmptyDirNode()
	err := rnode.SetCidBuilder(a.cidBuilder)
	if err != nil {
		return nil, err
	}
	mr, err := mfs.NewRoot(a.ctx, a.dagService, rnode, nil)
	if err != nil {
		return nil, err
	}
	a.mroot = mr
	return a.mroot, nil
}

func (a *adder) addFileNode(ctx context.Context, path string, file files.Node, toplevel bool) error {
	defer file.Close()

	if a.liveNodes >= liveCacheSize {
		mr, err := a.mfsRoot()
		if err != nil {
			return err
		}
		if err := mr.FlushMemFree(a.ctx); err != nil {
			return err
		}

		a.liveNodes = 0
	}
	a.liveNodes++

	switch f := file.(type) {
	case files.Directory:
		return a.addDir(ctx, path, f, toplevel)
	case *files.Symlink:
		return a.addSymlink(path, f)
	case files.File:
		return a.addFile(path, f)
	default:
		return errors.New("unknown file type")
	}
}

func (a *adder) addFile(path string, file files.File) error {
	size, err := file.Size()
	if err != nil {
		return err
	}

	// if the progress flag was specified, wrap the file so that we can send
	// progress updates to the client (over the output channel)
	var reader io.Reader = file

	var name string
	if path == "" {
		name = a.baseName
	} else {
		name = path
	}

	if a.Out != nil {
		rdr := &progressReader{file: reader, path: name, out: a.Out, size: size}
		if fi, ok := file.(files.FileInfo); ok {
			reader = &progressReader2{rdr, fi}
		} else {
			reader = rdr
		}
	}

	dagnode, err := a.add(reader)
	if err != nil {
		return err
	}

	// patch it into the root
	return a.addNode(dagnode, path)
}

func (a *adder) addDir(ctx context.Context, dirPath string, dir files.Directory, toplevel bool) error {
	if !(toplevel && dirPath == "") {
		mr, err := a.mfsRoot()
		if err != nil {
			return err
		}
		err = mfs.Mkdir(mr, dirPath, mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: a.cidBuilder,
		})
		if err != nil {
			return err
		}
	}

	it := dir.Entries()
	for it.Next() {
		fpath := path.Join(dirPath, it.Name())
		err := a.addFileNode(ctx, fpath, it.Node(), false)
		if err != nil {
			return err
		}
	}

	return it.Err()
}

func (a *adder) addSymlink(path string, l *files.Symlink) error {
	sdata, err := unixfs.SymlinkData(l.Target)
	if err != nil {
		return err
	}

	dagnode := merkledag.NodeWithData(sdata)
	err = dagnode.SetCidBuilder(a.cidBuilder)
	if err != nil {
		return err
	}
	err = a.dagService.Add(a.ctx, dagnode)
	if err != nil {
		return err
	}

	return a.addNode(dagnode, path)
}

// Constructs a node from reader's data, and adds it
func (a *adder) add(reader io.Reader) (ipld.Node, error) {
	chnk := chunk.NewSizeSplitter(reader, a.chunkSize)

	params := helpers.DagBuilderParams{
		Maxlinks:   helpers.DefaultLinksPerBlock, // Default max of 174 links per block
		RawLeaves:  true,                         // Leave the actual file bytes untouched instead of wrapping them in a dag-pb protobuf wrapper
		CidBuilder: a.cidBuilder,                 // Use CIDv1 for all links
		Dagserv:    a.bufferedDS,
		NoCopy:     false,
	}

	db, err := params.New(chnk)
	if err != nil {
		return nil, err
	}

	nd, err := balanced.Layout(db)
	if err != nil {
		return nil, err
	}

	return nd, a.bufferedDS.Commit()
}

func (a *adder) addNode(node ipld.Node, filePath string) error {
	// patch it into the root
	if filePath == "" {
		filePath = a.baseName
	}

	mr, err := a.mfsRoot()
	if err != nil {
		return err
	}
	dir := path.Dir(filePath)
	if dir != "." {
		opts := mfs.MkdirOpts{
			Mkparents:  true,
			Flush:      false,
			CidBuilder: a.cidBuilder,
		}
		if err := mfs.Mkdir(mr, dir, opts); err != nil {
			return err
		}
	}

	if err := mfs.PutNode(mr, filePath, node); err != nil {
		return err
	}

	return nil
}
