package models

import "github.com/theQRL/qrl-rich-list-indexer/common"

type Account struct {
	Address common.Address `json:"address" bson:"address"`
	Balance int64          `json:"amount" bson:"amount"`
}

func (a *Account) UpdateBalance(balance int64) {
	a.Balance += balance
}
