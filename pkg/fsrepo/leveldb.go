package fsrepo

import (
	"fmt"
	levelds "github.com/ipfs/go-ds-leveldb"
	ldbopts "github.com/syndtr/goleveldb/leveldb/opt"
	"path/filepath"
)

type levelDBDatastoreConfig struct {
	path        string
	compression ldbopts.Compression
}

func LevelDBDatastoreConfigParser() ConfigFromMap {
	return func(params map[string]interface{}) (DatastoreConfig, error) {
		var c levelDBDatastoreConfig
		var ok bool

		c.path, ok = params["path"].(string)
		if !ok {
			return nil, fmt.Errorf("'path' field is missing or not string")
		}

		switch cm := params["compression"]; cm {
		case "none":
			c.compression = ldbopts.NoCompression
		case "snappy":
			c.compression = ldbopts.SnappyCompression
		case "", nil:
			c.compression = ldbopts.DefaultCompression
		default:
			return nil, fmt.Errorf("unrecognized value for compression: %s", cm)
		}

		return &c, nil
	}
}

func (c *levelDBDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type": "levelds",
		"path": c.path,
	}
}

func (c *levelDBDatastoreConfig) Create(path string) (Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	return levelds.NewDatastore(p, &levelds.Options{
		Compression: c.compression,
	})
}
