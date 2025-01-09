package chunker

import "github.com/ipfs/boxo/ipld/unixfs/importer/helpers"

const maxChunkSize int = 1024 * 1024 * 10

var liveCacheSize = uint64(256 << 10)

const (
	Chunk1MiB  = 1024 * 1024
	Chunk10MiB = 1024 * 1024 * 10
)

func init() {
	helpers.BlockSizeLimit = maxChunkSize
}
