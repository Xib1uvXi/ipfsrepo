package fsrepo

import (
	"context"
	"errors"
	"fmt"
	ds "github.com/ipfs/go-datastore"
	measure "github.com/ipfs/go-ds-measure"
	lockfile "github.com/ipfs/go-fs-lock"
	"github.com/mitchellh/go-homedir"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const LockFile = "hificloud.repo.lock"

type FSRepo struct {
	locker sync.Mutex

	// has Close been called already
	closed bool

	// path is the file-system path
	path string

	// lockfile is the file system lock to prevent others from opening
	// the same fsrepo path concurrently
	lockfile io.Closer

	ds Datastore
}

func NewFSRepo(repoPath string) (*FSRepo, error) {

	if err := Init(repoPath); err != nil {
		return nil, err
	}

	reposrv, err := open(repoPath)
	if err != nil {
		return nil, err
	}

	return reposrv, nil
}

func (r *FSRepo) Datastore() Datastore {
	r.locker.Lock()
	defer r.locker.Unlock()

	d := r.ds
	return d
}

func (r *FSRepo) Path() string {
	return r.path
}

// GetStorageUsage computes the storage space taken by the repo in bytes.
func (r *FSRepo) GetStorageUsage(ctx context.Context) (uint64, error) {
	return ds.DiskUsage(ctx, r.Datastore())
}

// Close closes the FSRepo, releasing held resources.
func (r *FSRepo) Close() error {
	r.locker.Lock()
	defer r.locker.Unlock()

	if r.closed {
		return errors.New("repo is closed")
	}

	if err := r.ds.Close(); err != nil {
		return err
	}

	r.closed = true
	return r.lockfile.Close()
}

func (r *FSRepo) readSpec() (string, error) {
	fn := DatastoreSpec(r.path)
	b, err := os.ReadFile(fn)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(string(b)), nil
}

// openDatastore returns an error if the config file is not present.
func (r *FSRepo) openDatastore() error {
	dsc, err := AnyDatastoreConfig(DefaultDiskSpec())
	if err != nil {
		return err
	}
	spec := dsc.DiskSpec()

	oldSpec, err := r.readSpec()
	if err != nil {
		return err
	}
	if oldSpec != spec.String() {
		return fmt.Errorf("datastore configuration of '%s' does not match what is on disk '%s'",
			oldSpec, spec.String())
	}

	d, err := dsc.Create(r.path)
	if err != nil {
		return err
	}
	r.ds = d

	// Wrap it with metrics gathering
	prefix := "ipfs.fsrepo.datastore"
	r.ds = measure.New(prefix, r.ds)

	return nil
}
func Init(repoPath string) error {
	if err := initSpec(repoPath, DefaultDiskSpec()); err != nil {
		return err
	}

	return nil
}

func initSpec(path string, conf map[string]interface{}) error {
	fn := DatastoreSpec(path)

	if FileExists(fn) {
		return nil
	}

	dsc, err := AnyDatastoreConfig(conf)
	if err != nil {
		return err
	}
	bytes := dsc.DiskSpec().Bytes()

	return os.WriteFile(fn, bytes, 0o600)
}

func newFSRepo(repoPath string) (*FSRepo, error) {
	if repoPath == "" {
		return nil, errors.New("no repo path provided")
	}
	expPath, err := homedir.Expand(filepath.Clean(repoPath))
	if err != nil {
		return nil, err
	}

	return &FSRepo{path: expPath}, nil
}

func open(repoPath string) (*FSRepo, error) {
	r, err := newFSRepo(repoPath)
	if err != nil {
		return nil, err
	}

	r.locker.Lock()
	defer r.locker.Unlock()

	r.lockfile, err = lockfile.Lock(r.path, LockFile)
	if err != nil {
		return nil, err
	}
	keepLocked := false
	defer func() {
		// unlock on error, leave it locked on success
		if !keepLocked {
			r.lockfile.Close()
		}
	}()

	// check repo path, then check all constituent parts.
	if err := Writable(r.path); err != nil {
		return nil, err
	}

	if err := r.openDatastore(); err != nil {
		return nil, err
	}

	keepLocked = true
	return r, nil
}
