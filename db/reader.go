package db

import (
	"github.com/theQRL/qrl-rich-list-indexer/common"
	"github.com/theQRL/qrl-rich-list-indexer/db/models"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func (m *MongoDBProcessor) GetLastBlock() (*models.Block, error) {
	o := &options.FindOneOptions{}
	o.Sort = bson.D{{"number", -1}}

	result := m.blocksCollection.FindOne(m.ctx, bson.D{{}}, o)

	if result.Err() != nil {
		return nil, result.Err()
	}

	b := &models.Block{}
	err := result.Decode(b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (m *MongoDBProcessor) GetBlockByNumber(number int64) (*models.Block, error) {
	o := &options.FindOneOptions{}
	o.Sort = bson.D{{"number", -1}}

	result := m.blocksCollection.FindOne(m.ctx,
		bson.D{{"number", number}}, o)

	if result.Err() != nil {
		return nil, result.Err()
	}

	b := &models.Block{}
	err := result.Decode(b)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (m *MongoDBProcessor) GetAccountByAddress(address common.Address) (*models.Account, error) {
	o := &options.FindOneOptions{}
	o.Sort = bson.D{{"address", -1}}

	result := m.accountsCollection.FindOne(m.ctx,
		bson.D{{"address", address}}, o)

	if result.Err() == mongo.ErrNoDocuments {
		return &models.Account{}, nil
	} else if result.Err() != nil {
		return nil, result.Err()
	}

	a := &models.Account{}
	err := result.Decode(a)
	if err != nil {
		return nil, err
	}
	return a, nil
}

func (m *MongoDBProcessor) GetBalanceChangeLogsByBlockNumber(blockNumber int64) ([]*models.BalanceChangeLog, error) {
	var balanceChangeLogs []*models.BalanceChangeLog

	o := &options.FindOptions{}
	o.Sort = bson.D{{"blockNumber", -1}}

	cursor, err := m.balanceChangeLogsCollection.Find(m.ctx,
		bson.D{{"blockNumber", blockNumber}}, o)
	if err != nil {
		return nil, err
	}

	for cursor.Next(m.ctx) {
		t := &models.BalanceChangeLog{}
		err = cursor.Decode(t)
		if err != nil {
			return nil, err
		}
		balanceChangeLogs = append(balanceChangeLogs, t)
	}

	return balanceChangeLogs, nil
}
