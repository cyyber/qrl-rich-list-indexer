package cache

import (
	"github.com/theQRL/qrl-rich-list-indexer/common"
	"github.com/theQRL/qrl-rich-list-indexer/db/models"
)

type AccountCache map[common.Address]*models.Account

func (a AccountCache) Get(address common.Address) *models.Account {
	return a[address]
}

func (a AccountCache) Put(address common.Address, value *models.Account) {
	a[address] = value
}
