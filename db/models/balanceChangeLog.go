package models

import (
	"github.com/theQRL/qrl-rich-list-indexer/common"
)

type BalanceChangeLog struct {
	BlockNumber int64          `json:"blockNumber" bson:"blockNumber"`
	Address     common.Address `json:"from" bson:"from"`
	DeltaAmount int64          `json:"deltaAmount" bson:"deltaAmount"` // Change in amount it will be positive if amount increased and negative if amount decreased
}

func (b *BalanceChangeLog) UpdateDeltaAmount(deltaAmount int64) {
	b.DeltaAmount += deltaAmount
}

func NewBalanceChangeLog(blockNumber int64, address common.Address) *BalanceChangeLog {
	return &BalanceChangeLog{
		BlockNumber: blockNumber,
		Address:     address,
	}
}
