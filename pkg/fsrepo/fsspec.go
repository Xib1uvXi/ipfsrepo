package fsrepo

import (
	"bytes"
	"encoding/json"
)

func DefaultDiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "mount",
		"mounts": []interface{}{
			map[string]interface{}{
				"mountpoint": "/blocks",
				"type":       "measure",
				"prefix":     "flatfs.datastore",
				"child": map[string]interface{}{
					"type":      "flatfs",
					"path":      "blocks",
					"sync":      true,
					"shardFunc": "/repo/flatfs/shard/v1/next-to-last/2",
				},
			},
			map[string]interface{}{
				"mountpoint": "/",
				"type":       "measure",
				"prefix":     "leveldb.datastore",
				"child": map[string]interface{}{
					"type":        "levelds",
					"path":        "datastore",
					"compression": "none",
				},
			},
		},
	}
}

type DiskSpec map[string]interface{}

// Bytes returns a minimal JSON encoding of the DiskSpec.
func (spec DiskSpec) Bytes() []byte {
	b, err := json.Marshal(spec)
	if err != nil {
		// should not happen
		panic(err)
	}
	return bytes.TrimSpace(b)
}

// String returns a minimal JSON encoding of the DiskSpec.
func (spec DiskSpec) String() string {
	return string(spec.Bytes())
}
