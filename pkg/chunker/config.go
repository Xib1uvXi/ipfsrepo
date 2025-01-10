package chunker

import (
	"github.com/dustin/go-humanize"
	"github.com/ipfs/boxo/ipld/unixfs/importer/helpers"
)

const maxChunkSize int = 1024 * 1024 * 10

var liveCacheSize = uint64(256 << 10)

const (
	Chunk1MiB  = 1024 * 1024
	Chunk10MiB = 1024 * 1024 * 10
)

func init() {
	helpers.BlockSizeLimit = maxChunkSize
}

func GetChunkSize(size int) string {
	switch size {
	case Chunk1MiB:
		return "1MiB"
	case Chunk10MiB:
		return "10MiB"

	default:
		return humanize.Bytes(uint64(size))
	}
}
