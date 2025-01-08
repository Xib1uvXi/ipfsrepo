package fsrepo

import (
	"fmt"
	flatfs "github.com/ipfs/go-ds-flatfs"
	"path/filepath"
)

type flatFsDatastoreConfig struct {
	path      string
	shardFun  *flatfs.ShardIdV1
	syncField bool
}

func (c *flatFsDatastoreConfig) DiskSpec() DiskSpec {
	return map[string]interface{}{
		"type":      "flatfs",
		"path":      c.path,
		"shardFunc": c.shardFun.String(),
	}
}

func (c *flatFsDatastoreConfig) Create(path string) (Datastore, error) {
	p := c.path
	if !filepath.IsAbs(p) {
		p = filepath.Join(path, p)
	}

	return flatfs.CreateOrOpen(p, c.shardFun, c.syncField)
}

func FlatFsDatastoreConfigParser() ConfigFromMap {
	return func(params map[string]interface{}) (DatastoreConfig, error) {
		var c flatFsDatastoreConfig
		var ok bool
		var err error

		c.path, ok = params["path"].(string)
		if !ok {
			return nil, fmt.Errorf("'path' field is missing or not boolean")
		}

		sshardFun, ok := params["shardFunc"].(string)
		if !ok {
			return nil, fmt.Errorf("'shardFunc' field is missing or not a string")
		}
		c.shardFun, err = flatfs.ParseShardFunc(sshardFun)
		if err != nil {
			return nil, err
		}

		c.syncField, ok = params["sync"].(bool)
		if !ok {
			return nil, fmt.Errorf("'sync' field is missing or not boolean")
		}
		return &c, nil
	}
}
