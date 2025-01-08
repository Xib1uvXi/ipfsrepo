package chunker

const maxChunkSize int64 = 1024 * 1024 * 10

var liveCacheSize = uint64(256 << 10)

const (
	Chunk1MiB  = 1024 * 1024
	Chunk10MiB = 1024 * 1024 * 10
)
