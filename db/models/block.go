package models

import (
	"github.com/theQRL/qrl-rich-list-indexer/common"
	"github.com/theQRL/qrl-rich-list-indexer/generated"
	"github.com/theQRL/qrl-rich-list-indexer/misc"
)

type Block struct {
	Number int64       `json:"number" bson:"number"`
	Hash   common.Hash `json:"hash" bson:"hash"`
}

func NewBlockFromPBData(pbBlock *generated.Block) *Block {
	return &Block{
		Number: int64(pbBlock.Header.BlockNumber),
		Hash:   misc.ToSizedHash(pbBlock.Header.HashHeader),
	}
}

func (b *Block) GetNumber() uint64 {
	return uint64(b.Number)
}
