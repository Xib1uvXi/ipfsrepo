package ipfsrepo

import (
	"context"
	"github.com/ipfs/boxo/blockstore"
	blocks "github.com/ipfs/go-block-format"
	cid2 "github.com/ipfs/go-cid"
)

type BlockRepo struct {
	blockStore blockstore.Blockstore
}

// SaveBlock save block to blockstore
func (b *BlockRepo) SaveBlock(ctx context.Context, data [][]byte) error {
	var blks []blocks.Block
	for _, d := range data {
		blk := blocks.NewBlock(d)
		blks = append(blks, blk)
	}

	return b.blockStore.PutMany(ctx, blks)
}

// HasBlock check if block exists in blockstore
func (b *BlockRepo) HasBlock(ctx context.Context, cids []string) bool {
	var result bool = true
	for _, blockCidStr := range cids {
		select {
		case <-ctx.Done():
			return false

		default:
			cid, err := cid2.Parse(blockCidStr)
			if err != nil {
				return false
			}

			has, err := b.blockStore.Has(ctx, cid)
			if err != nil {
				return false
			}

			if !has {
				result = false
				break
			}
		}
	}

	return result
}

// DeleteBlock delete block from blockstore
func (b *BlockRepo) DeleteBlock(ctx context.Context, cids []string) error {
	for _, cidStr := range cids {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		cid, err := cid2.Parse(cidStr)
		if err != nil {
			return err
		}

		if err := b.blockStore.DeleteBlock(ctx, cid); err != nil {
			return err
		}
	}

	return nil
}

// DeleteAllBlocks delete all blocks from blockstore
func (b *BlockRepo) DeleteAllBlocks(ctx context.Context) error {
	kch, err := b.blockStore.AllKeysChan(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case k, ok := <-kch:
			if !ok {
				return nil
			}
			if err := b.blockStore.DeleteBlock(ctx, k); err != nil {
				return err
			}
		}
	}
}
