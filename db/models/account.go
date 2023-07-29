package models

import "github.com/theQRL/qrl-rich-list-indexer/common"

type Account struct {
	Address common.Address `json:"address" bson:"address"`
	Balance int64          `json:"balance" bson:"balance"`
}

func (a *Account) UpdateBalance(balance int64) {
	a.Balance += balance
}

func NewAccount(address common.Address) *Account {
	return &Account{
		Address: address,
		Balance: 0,
	}
}
