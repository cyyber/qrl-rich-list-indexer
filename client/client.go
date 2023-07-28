package client

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"reflect"
	"sync"
	"time"

	"github.com/theQRL/qrl-rich-list-indexer/common"
	"github.com/theQRL/qrl-rich-list-indexer/config"
	"github.com/theQRL/qrl-rich-list-indexer/db"
	"github.com/theQRL/qrl-rich-list-indexer/db/models"
	"github.com/theQRL/qrl-rich-list-indexer/generated"
	"github.com/theQRL/qrl-rich-list-indexer/log"
	"go.mongodb.org/mongo-driver/mongo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type QRLIndexer struct {
	conn *grpc.ClientConn
	pac  generated.PublicAPIClient

	lock sync.Mutex
	wg   sync.WaitGroup

	log log.LoggerInterface

	config *config.Config

	m *db.MongoDBProcessor

	quit       chan struct{}
	disconnect bool
}

func ConnectServer(m *db.MongoDBProcessor) (*QRLIndexer, error) {
	c := config.GetConfig()
	qrlNodeConfig := c.GetQRLNodeConfig()
	conn, err := grpc.Dial(fmt.Sprintf("%s:%d", qrlNodeConfig.IP, qrlNodeConfig.PublicAPIPort),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}

	pac := generated.NewPublicAPIClient(conn)
	m.SetPAC(pac)
	nc := &QRLIndexer{
		conn:   conn,
		pac:    pac,
		config: c,
		log:    log.GetLogger(),
		m:      m,
		quit:   make(chan struct{}),
	}
	return nc, nil
}

func (qi *QRLIndexer) Start() {
	qi.lock.Lock()
	defer qi.lock.Unlock()

	go qi.run()
}

func (qi *QRLIndexer) Stop() {
	qi.Disconnect()
}

func (qi *QRLIndexer) close() {
	qi.lock.Lock()
	defer qi.lock.Unlock()

	close(qi.quit)

	qi.conn.Close()
}

func (qi *QRLIndexer) Disconnect() {
	qi.log.Info("Disconnecting...")
	qi.disconnect = true
	qi.close()
	qi.wg.Wait()
}

func (qi *QRLIndexer) run() (err error) {
	qi.wg.Add(1)
	defer qi.wg.Done()
loop:
	for {
		select {
		case <-time.After(10 * time.Second):
			height := uint64(common.BLOCKZERO)
			b, err := qi.m.GetLastBlock()
			// If last block not found, then request for genesis block and process it
			if err == mongo.ErrNoDocuments {
				block, err := qi.requestForBlockByNumber(height)
				if err != nil {
					qi.log.Error("[run] Error requestForBlockByNumber",
						"Blocknumber", height,
						"Error", err.Error())
					return err
				}
				err = qi.m.ProcessBlock(block)
				if err != nil {
					qi.log.Error("[run] Failed to ProcessBlock (genesis)",
						"#", block.Header.BlockNumber,
						"Hash", hex.EncodeToString(block.Header.HashHeader),
						"Error", err.Error())
					return err
				}
				qi.log.Info("Successfully Processed Genesis Block")
				continue
			} else if err != nil {
				qi.log.Error("[run] Error in GetLastBlock",
					"Error", err.Error())
				return err
			} else if b == nil {
				err = errors.New("GetLastBlock returned nil")
				qi.log.Error("[run] Unexpected Error", "Error", err.Error())
				return err
			}

			height = b.GetNumber()
			// Request the block at current height
			block, err := qi.requestForBlockByNumber(height)
			if err != nil {
				qi.log.Error("[run] Error requestForBlockByNumber",
					"Blocknumber", height,
					"Error", err.Error())
				return err
			}

			if block == nil || !reflect.DeepEqual(block.Header.HashHeader, b.Hash[:]) {
				err = qi.Rollback(b)
				if err != nil {
					qi.log.Error("[run] Failed to Rollback",
						"Error", err.Error())
					return err
				}
				continue
			}

			for !qi.disconnect {
				b, err = qi.m.GetLastBlock()
				if err != nil {
					qi.log.Error("[run] Error in GetLastBlock",
						"Error", err.Error())
					return err
				}
				block, err = qi.requestForBlockByNumber(height + 1)
				if err != nil {
					qi.log.Error("[run] Error requestForBlockByNumber while syncing",
						"#", height,
						"Error", err.Error())
					return err
				}

				// Syncing finished if we cannot find the next block
				if block == nil {
					qi.log.Info("No block found for ", "height", height+1)
					break
				}

				if !reflect.DeepEqual(b.Hash[:], block.Header.HashHeaderPrev) {
					// Break as it is the case of fork recovery, and recovery will happen in next iteration
					qi.log.Info("fork found")
					qi.log.Info("MongoDB block", "#", b.Number, "hash", b.Hash.ToString())
					qi.log.Info("Node block", "#", block.Header.BlockNumber,
						"prev hash", hex.EncodeToString(block.Header.HashHeaderPrev))
					break
				}

				err = qi.m.ProcessBlock(block)
				if err != nil {
					qi.log.Error("[run] Failed to ProcessBlock",
						"#", block.Header.BlockNumber,
						"Hash", hex.EncodeToString(block.Header.HashHeader),
						"Error", err.Error())
					return err
				}
				height = block.Header.BlockNumber
			}
		case <-qi.quit:
			break loop
		}
	}
	return err
}

func (qi *QRLIndexer) GetAddrFromTx(tx *generated.Transaction) []byte {
	if tx.MasterAddr != nil {
		return tx.MasterAddr
	}
	//return misc.UCharVectorToBytes(goqrllib.QRLHelperGetAddress(misc.BytesToUCharVector(tx.PublicKey)))
	return nil

}

func (qi *QRLIndexer) requestForBlockByNumber(blockNumber uint64) (*generated.Block, error) {
	qi.log.Info("Request block ", "#", blockNumber)
	resp, err := qi.pac.GetBlockByNumber(context.Background(),
		&generated.GetBlockByNumberReq{BlockNumber: blockNumber})

	if err != nil {
		return nil, err
	}

	return resp.Block, err
}

func (qi *QRLIndexer) requestForBlockHeight() (uint64, error) {
	resp, err := qi.pac.GetHeight(context.Background(),
		&generated.GetHeightReq{})

	if err != nil {
		return 0, err
	}

	return resp.Height, err
}

func (qi *QRLIndexer) Rollback(b *models.Block) error {
	qi.log.Info("Rollback triggered due to block",
		"#", b.Number,
		"hash", b.Hash.ToString())

	for b.Number != common.BLOCKZERO {
		err := qi.m.RevertLastBlock()
		if err != nil {
			qi.log.Error("[Rollback] Error in RevertLastBlock",
				"Error", err)
		}

		b, err = qi.m.GetLastBlock()
		if err != nil {
			qi.log.Error("[run] Error in GetLastBlock",
				"Error", err.Error())
			return err
		}

		block, err := qi.requestForBlockByNumber(uint64(b.Number))
		if err != nil {
			qi.log.Error("[run] Error requestForBlockByNumber",
				"#", b.Number,
				"Error", err.Error())
			return err
		}

		if reflect.DeepEqual(block.Header.HashHeader, b.Hash[:]) {
			break
		}
	}

	qi.log.Info("Rollback finished")
	return nil
}
