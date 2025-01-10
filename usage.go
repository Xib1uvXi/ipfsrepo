package ipfsrepo

import (
	"context"
	"github.com/dustin/go-humanize"
	"os"
	"path/filepath"
	"time"
)

const (
	defaultScanInterval = 5 * time.Minute
	defaultThreshold    = 70.0
)

type StorageUsage struct {
	ctx          context.Context
	repoPath     string
	maxStorage   uint64
	usage        uint64
	scanInterval time.Duration
	threshold    float64
}

func (s *StorageUsage) SetScanInterval(scanInterval time.Duration) {
	s.scanInterval = scanInterval
}

func (s *StorageUsage) SetThreshold(threshold float64) {
	s.threshold = threshold
}

func NewStorageUsage(ctx context.Context, repoPath string, maxStorage uint64) (*StorageUsage, error) {
	s := &StorageUsage{
		ctx:          ctx,
		repoPath:     repoPath,
		maxStorage:   maxStorage,
		scanInterval: defaultScanInterval,
		threshold:    defaultThreshold,
	}

	size, err := s.getStorageUsage(repoPath) // initial scan
	if err != nil {
		return nil, err
	}

	s.usage = size

	return s, nil
}

// Start starts the storage usage monitoring
func (s *StorageUsage) Start() {
	go s.loop()
}

// MaxStorage returns the maximum storage size of the repo
func (s *StorageUsage) MaxStorage() string {
	return humanize.Bytes(s.maxStorage)
}

// Usage returns the current storage usage of the repo
func (s *StorageUsage) Usage() string {
	return humanize.Bytes(s.usage)
}

// UsagePercentage returns the current storage usage percentage of the repo
func (s *StorageUsage) UsagePercentage() float64 {
	return float64(s.usage) / float64(s.maxStorage) * 100
}

// IsFull returns true if the repo is full
func (s *StorageUsage) IsFull() bool {
	return s.UsagePercentage() >= s.threshold
}

// loop is a background goroutine that periodically scans the repo for storage usage
func (s *StorageUsage) loop() {
	ticker := time.NewTicker(s.scanInterval)
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			usage, err := s.getStorageUsage(s.repoPath)
			if err != nil {
				continue
			}

			s.usage = usage
		}
	}
}

// getStorageUsage returns the current storage usage of the repo
func (s *StorageUsage) getStorageUsage(checkPath string) (uint64, error) {
	var usage uint64

	err := filepath.Walk(checkPath, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		usage += uint64(info.Size())
		return nil
	})

	if err != nil {
		return 0, err
	}

	return usage, nil
}
