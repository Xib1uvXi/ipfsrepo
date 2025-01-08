package chunker

import (
	"github.com/ipfs/boxo/files"
	"io"
)

const progressReaderIncrement = 1024 * 1024

type AddEvent struct {
	Name  string `json:"name"`
	Bytes int64  `json:",omitempty"`
	Size  int64  `json:",omitempty"`
}

type progressReader struct {
	file         io.Reader
	path         string
	out          chan<- interface{}
	bytes        int64
	lastProgress int64
	size         int64
}

func (i *progressReader) Read(p []byte) (int, error) {
	n, err := i.file.Read(p)

	i.bytes += int64(n)
	if i.bytes-i.lastProgress >= progressReaderIncrement || err == io.EOF {
		i.lastProgress = i.bytes
		i.out <- &AddEvent{
			Name:  i.path,
			Bytes: i.bytes,
			Size:  i.size,
		}
	}

	return n, err
}

type progressReader2 struct {
	*progressReader
	files.FileInfo
}

func (i *progressReader2) Read(p []byte) (int, error) {
	return i.progressReader.Read(p)
}
