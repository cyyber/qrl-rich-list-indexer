package cache

import (
	"github.com/theQRL/qrl-rich-list-indexer/common"
	"github.com/theQRL/qrl-rich-list-indexer/db/models"
)

type BalanceChangeLogCache map[common.Address]*models.BalanceChangeLog

func (b BalanceChangeLogCache) Get(address common.Address) *models.BalanceChangeLog {
	return b[address]
}

func (b BalanceChangeLogCache) Put(address common.Address, value *models.BalanceChangeLog) {
	b[address] = value
}

func (b BalanceChangeLogCache) Update(blockNumber int64, address common.Address, deltaAmount int64) {
	v := b.Get(address)
	if v == nil {
		v = models.NewBalanceChangeLog(blockNumber, address)
		b.Put(address, v)
	}
	v.UpdateDeltaAmount(deltaAmount)
}
