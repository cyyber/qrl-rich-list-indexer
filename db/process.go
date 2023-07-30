package db

import (
	"context"
	"encoding/hex"

	"github.com/theQRL/qrl-rich-list-indexer/cache"
	"github.com/theQRL/qrl-rich-list-indexer/common"
	"github.com/theQRL/qrl-rich-list-indexer/config"
	"github.com/theQRL/qrl-rich-list-indexer/db/models"
	"github.com/theQRL/qrl-rich-list-indexer/generated"
	"github.com/theQRL/qrl-rich-list-indexer/misc"
	"github.com/theQRL/qrl-rich-list-indexer/xmss"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

func AddInsertOneModelIntoOperations(operations *[]mongo.WriteModel, model interface{}) {
	operation := mongo.NewInsertOneModel()
	operation.SetDocument(model)
	*operations = append(*operations, operation)
}

func AddDeleteOneModelIntoOperations(operations *[]mongo.WriteModel, model interface{}) {
	operation := mongo.NewDeleteOneModel()
	operation.SetFilter(model)
	*operations = append(*operations, operation)
}

func (m *MongoDBProcessor) ProcessBlock(b *generated.Block) error {
	var blockOperations []mongo.WriteModel
	var accountOperations []mongo.WriteModel
	var balanceChangeLogOperations []mongo.WriteModel

	blockNumber := int64(b.Header.BlockNumber)
	blockModel := models.NewBlockFromPBData(b)
	AddInsertOneModelIntoOperations(&blockOperations, blockModel)

	reOrgLimit := common.BLOCKZERO + config.GetConfig().ReOrgLimit
	if uint64(blockModel.Number) > reOrgLimit {
		removeBlockNumber := uint64(blockModel.Number) - reOrgLimit
		deleteOneOperation := mongo.NewDeleteOneModel()
		deleteOneOperation.SetFilter(bsonx.Doc{
			{"number", bsonx.Int64(int64(removeBlockNumber))},
		})
		blockOperations = append(blockOperations, deleteOneOperation)

		deleteManyOperation := mongo.NewDeleteManyModel()
		deleteManyOperation.SetFilter(bsonx.Doc{
			{"blockNumber", bsonx.Int64(int64(removeBlockNumber))},
		})
		balanceChangeLogOperations = append(balanceChangeLogOperations, deleteManyOperation)
	}

	accountCache := make(cache.AccountCache)
	balanceChangeLogCache := make(cache.BalanceChangeLogCache)

	for _, protoTX := range b.Transactions {
		var addrFrom common.Address
		totalAmountSpent := int64(protoTX.Fee)

		switch protoTX.TransactionType.(type) {
		case *generated.Transaction_Coinbase:
		default:
			addrFrom = xmss.GetXMSSAddressFromPK(protoTX.PublicKey)
			if protoTX.MasterAddr != nil {
				addrFrom = misc.ToStringAddress(protoTX.MasterAddr)
			}
		}

		switch protoTX.TransactionType.(type) {
		case *generated.Transaction_Coinbase:
			coinBaseTX := protoTX.GetCoinbase()
			address := misc.ToStringAddress(coinBaseTX.AddrTo)
			amount := int64(coinBaseTX.Amount)

			err := m.UpdateAccountAndLog(blockNumber, address, amount, accountCache, balanceChangeLogCache)
			if err != nil {
				m.log.Error("[ProcessBlock] Failed to UpdateAccountAndLog for coinBase.AddrTo",
					"Error", err.Error())
				return err
			}
		case *generated.Transaction_Transfer_:
			transferTX := protoTX.GetTransfer()
			for i, addr := range transferTX.AddrsTo {
				address := misc.ToStringAddress(addr)
				amount := int64(transferTX.Amounts[i])
				totalAmountSpent += amount

				err := m.UpdateAccountAndLog(blockNumber, address, amount, accountCache, balanceChangeLogCache)
				if err != nil {
					m.log.Error("[ProcessBlock] Failed to UpdateAccountAndLog for transferTX.AddrsTo",
						"Error", err.Error())
					return err
				}
			}
		case *generated.Transaction_LatticePK:
		case *generated.Transaction_Message_:
		case *generated.Transaction_Token_:
		case *generated.Transaction_TransferToken_:
		case *generated.Transaction_Slave_:
		case *generated.Transaction_MultiSigCreate_:
		case *generated.Transaction_MultiSigSpend_:
		case *generated.Transaction_MultiSigVote_:
			// Get Vote Stats
			// if not executed ignore
			// Get maximum blocknumber from all tx_hashes in vote stats
			// check if current blocknumber == maximum blocknumber
			// if true, then get multisig spend and apply

			multiSigVoteTX := protoTX.GetMultiSigVote()
			respVoteStats, err := m.pac.GetVoteStats(context.Background(), &generated.GetVoteStatsReq{
				MultiSigSpendTxHash: multiSigVoteTX.SharedKey})
			if err != nil {
				m.log.Error("[ProcessBlock] Error calling GetVoteStats",
					"Error", err.Error())
				return err
			}
			if !respVoteStats.VoteStats.Executed {
				continue
			}
			maxBlockNumber := blockNumber
			for _, txHash := range respVoteStats.VoteStats.TxHashes {
				resp, err := m.pac.GetTransaction(context.Background(), &generated.GetTransactionReq{
					TxHash: txHash})
				if err != nil {
					m.log.Error("[ProcessBlock] Error calling GetTransaction",
						"Error", err.Error())
					return err
				}
				innerTXBlockNumber := int64(resp.GetBlockNumber())
				// Found a transaction at higher block number, so no need to process this transaction
				if innerTXBlockNumber > maxBlockNumber {
					maxBlockNumber = innerTXBlockNumber
					break
				}
			}
			if maxBlockNumber != blockNumber {
				continue
			}
			respGetTX, err := m.pac.GetTransaction(context.Background(), &generated.GetTransactionReq{
				TxHash: multiSigVoteTX.SharedKey})
			if err != nil {
				m.log.Error("[ProcessBlock] Error calling GetTransaction for multisig spend txn",
					"Error", err.Error())
				return err
			}
			multiSigTx := respGetTX.Tx.GetMultiSigSpend()
			totalAmountSpentByMultiSig := int64(0)
			for i, addr := range multiSigTx.AddrsTo {
				address := misc.ToStringAddress(addr)
				amount := int64(multiSigTx.Amounts[i])
				totalAmountSpentByMultiSig += amount

				err := m.UpdateAccountAndLog(blockNumber, address, amount, accountCache, balanceChangeLogCache)
				if err != nil {
					m.log.Error("[ProcessBlock] Failed to UpdateAccountAndLog for multiSigTx.AddrsTo",
						"Error", err.Error())
					return err
				}
			}

			multiSigAddress := misc.ToStringAddress(multiSigTx.MultiSigAddress)
			err = m.UpdateAccountAndLog(blockNumber, multiSigAddress, totalAmountSpentByMultiSig*-1,
				accountCache, balanceChangeLogCache)
			if err != nil {
				m.log.Error("[ProcessBlock] Failed to UpdateAccountAndLog for multiSigAddress",
					"Error", err.Error())
				return err
			}
		default:
			continue
		}

		if len(addrFrom) != 0 {
			err := m.UpdateAccountAndLog(blockNumber, addrFrom, totalAmountSpent*-1,
				accountCache, balanceChangeLogCache)
			if err != nil {
				m.log.Error("[ProcessBlock] Failed to UpdateAccountAndLog for addrFrom",
					"Error", err.Error())
				return err
			}
		}
	}

	if uint64(blockNumber) >= m.config.BanStartBlockNumber {
		for addr, _ := range m.config.BannedQRLAddressList {
			account := models.NewAccount(common.Address(addr))

			operation := mongo.NewUpdateOneModel()
			operation.SetUpsert(true)
			operation.SetFilter(bsonx.Doc{
				{"address", bsonx.String(addr)},
			})
			operation.SetUpdate(bson.M{"$set": account})
			accountOperations = append(accountOperations, operation)
		}
	}

	for addr, balanceChangeLog := range balanceChangeLogCache {
		if _, ok := m.config.BannedQRLAddressList[addr.ToString()]; ok {
			if uint64(blockNumber) >= m.config.BanStartBlockNumber {
				continue
			}
		}
		AddInsertOneModelIntoOperations(&balanceChangeLogOperations, balanceChangeLog)
	}

	session, err := m.client.StartSession(options.Session())
	if err != nil {
		m.log.Error("[ProcessBlock] failed to start session")
		return err
	}
	defer session.EndSession(m.ctx)

	err = mongo.WithSession(m.ctx, session, func(sctx mongo.SessionContext) error {
		if err := sctx.StartTransaction(); err != nil {
			return err
		}

		if _, err := m.blocksCollection.BulkWrite(sctx, blockOperations); err != nil {
			m.log.Error("Failed to write in blocksCollection",
				"total operations", len(blockOperations))
			return err
		}

		if len(accountOperations) > 0 {
			if _, err := m.accountsCollection.BulkWrite(sctx, accountOperations); err != nil {
				m.log.Error("Failed to write in accountsCollection",
					"total operations", len(accountOperations))
				return err
			}
		}
		if len(balanceChangeLogOperations) > 0 {
			if _, err := m.balanceChangeLogsCollection.BulkWrite(sctx, balanceChangeLogOperations); err != nil {
				m.log.Error("Failed to write in balanceChangeLogsCollection",
					"total operations", len(balanceChangeLogOperations))
				return err
			}
		}
		return sctx.CommitTransaction(sctx)
	})
	if err != nil {
		m.log.Info("Failed to Process",
			"Block #", b.Header.BlockNumber,
			"HeaderHash", hex.EncodeToString(b.Header.HashHeader),
			"Error", err)
		return err
	}

	m.log.Info("Processed",
		"Block #", b.Header.BlockNumber,
		"HeaderHash", hex.EncodeToString(b.Header.HashHeader))
	return nil
}

func (m *MongoDBProcessor) RevertLastBlock() error {
	b, err := m.GetLastBlock()
	if err != nil {
		m.log.Error("[RevertLastBlock] failed to get last block",
			"error", err)
		return err
	}

	var blockOperations []mongo.WriteModel
	var accountOperations []mongo.WriteModel
	var balanceChangeLogOperations []mongo.WriteModel

	var operation *mongo.UpdateOneModel
	var deleteManyOperation *mongo.DeleteManyModel

	accountCache := make(cache.AccountCache)
	balanceChangeLogCache := make(cache.BalanceChangeLogCache)

	balanceChangeLogs, err := m.GetBalanceChangeLogsByBlockNumber(b.Number)
	if err != nil {
		m.log.Error("[RevertLastBlock] Error calling GetTokenHolders",
			"Error", err.Error())
		return err
	}

	for _, balanceChangeLog := range balanceChangeLogs {
		err := m.UpdateAccountAndLog(b.Number, balanceChangeLog.Address, balanceChangeLog.DeltaAmount*-1, accountCache, balanceChangeLogCache)
		if err != nil {
			m.log.Error("[ProcessBlock] Failed to UpdateAccountAndLog for coinBase.AddrTo",
				"Error", err.Error())
			return err
		}

		a := accountCache[balanceChangeLog.Address]
		operation = mongo.NewUpdateOneModel()
		operation.SetFilter(bsonx.Doc{
			{"address", bsonx.String(balanceChangeLog.Address.ToString())},
		})
		operation.SetUpdate(bson.M{"$set": a})
		accountOperations = append(accountOperations, operation)
	}

	deleteManyOperation = mongo.NewDeleteManyModel()
	deleteManyOperation.SetFilter(bsonx.Doc{
		{"blockNumber", bsonx.Int64(b.Number)},
	})
	balanceChangeLogOperations = append(balanceChangeLogOperations, deleteManyOperation)

	AddDeleteOneModelIntoOperations(&blockOperations, b)

	session, err := m.client.StartSession(options.Session())
	if err != nil {
		m.log.Error("[RevertLastBlock] failed to start session")
		return err
	}
	defer session.EndSession(m.ctx)

	err = mongo.WithSession(m.ctx, session, func(sctx mongo.SessionContext) error {
		if err := sctx.StartTransaction(); err != nil {
			return err
		}

		if _, err := m.blocksCollection.BulkWrite(sctx, blockOperations); err != nil {
			m.log.Error("Failed to write in blocksCollection",
				"total operations", len(blockOperations))
			return err
		}

		if len(accountOperations) > 0 {
			if _, err := m.accountsCollection.BulkWrite(sctx, accountOperations); err != nil {
				m.log.Error("Failed to write in accountsCollection",
					"total operations", len(accountOperations))
				return err
			}
		}
		if len(balanceChangeLogOperations) > 0 {
			if _, err := m.balanceChangeLogsCollection.BulkWrite(sctx, balanceChangeLogOperations); err != nil {
				m.log.Error("Failed to write in balanceChangeLogsCollection",
					"total operations", len(balanceChangeLogOperations))
				return err
			}
		}
		return sctx.CommitTransaction(sctx)
	})
	if err != nil {
		m.log.Info("Failed to Revert",
			"Block #", b.Number,
			"HeaderHash", b.Hash.ToString(),
			"Error", err)
		return err
	}

	m.log.Info("Reverted",
		"Block #", b.Number,
		"HeaderHash", b.Hash.ToString())
	return nil
}

func (m *MongoDBProcessor) GetAccountFromDBOrCache(address common.Address, ac cache.AccountCache) (*models.Account, error) {
	a, ok := ac[address]
	if ok {
		return a, nil
	}
	a, err := m.GetAccountByAddress(address)
	if err != nil {
		return nil, err
	}

	ac.Put(address, a)

	return a, nil
}
func (m *MongoDBProcessor) UpdateAccountAndLog(blockNumber int64, address common.Address,
	amount int64, accountCache cache.AccountCache, balanceChangeLogCache cache.BalanceChangeLogCache) error {
	a, err := m.GetAccountFromDBOrCache(address, accountCache)
	if err != nil {
		return err
	}
	a.UpdateBalance(amount)
	balanceChangeLogCache.Update(blockNumber, address, amount)

	return nil
}
