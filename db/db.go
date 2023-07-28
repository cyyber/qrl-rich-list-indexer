package db

import (
	"context"
	"fmt"
	"github.com/theQRL/qrl-rich-list-indexer/generated"
	"time"

	"github.com/theQRL/qrl-rich-list-indexer/config"
	"github.com/theQRL/qrl-rich-list-indexer/log"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/x/bsonx"
)

type MongoDBProcessor struct {
	client   *mongo.Client
	database *mongo.Database

	ctx    context.Context
	config *config.Config
	log    log.LoggerInterface

	pac generated.PublicAPIClient

	//lastBlock *Block
	blocksCollection            *mongo.Collection
	accountsCollection          *mongo.Collection
	balanceChangeLogsCollection *mongo.Collection
}

func (m *MongoDBProcessor) SetPAC(pac generated.PublicAPIClient) {
	m.pac = pac
}

func (m *MongoDBProcessor) IsDataBaseExists(dbName string) (bool, error) {
	databaseNames, err := m.client.ListDatabaseNames(m.ctx, bsonx.Doc{})
	if err != nil {
		return false, err
	}
	for i := range databaseNames {
		if databaseNames[i] == dbName {
			return true, nil
		}
	}
	return false, nil
}

func (m *MongoDBProcessor) IsCollectionExists(collectionName string) (bool, error) {
	cursor, err := m.database.ListCollections(m.ctx, bsonx.Doc{})
	if err != nil {
		return false, err
	}
	for cursor.Next(m.ctx) {
		next := &bsonx.Doc{}
		err := cursor.Decode(next)
		if err != nil {
			return false, err
		}
		//_, err = next.LookupErr(collectionName)
		elem, err := next.LookupErr("name")
		if err != nil {
			return false, nil
		}

		elemName := elem.StringValue()
		if elemName == collectionName {
			return true, nil
		}
	}
	return false, nil
}

func (m *MongoDBProcessor) CreateBlocksIndexes(found bool) error {
	m.blocksCollection = m.database.Collection("blocks")
	if found {
		return nil
	}
	_, err := m.blocksCollection.Indexes().CreateMany(context.Background(),
		[]mongo.IndexModel{
			{Keys: bson.M{"number": int32(-1)}},
			{Keys: bson.M{"hash": int32(-1)}},
		})
	if err != nil {
		m.log.Error("Error while modeling index for blocks",
			"Error", err)
		return err
	}
	return nil
}

func (m *MongoDBProcessor) CreateAccountsIndexes(found bool) error {
	m.accountsCollection = m.database.Collection("accounts")
	if found {
		return nil
	}
	_, err := m.accountsCollection.Indexes().CreateMany(context.Background(),
		[]mongo.IndexModel{
			{Keys: bson.M{"address": int32(-1)}},
		})
	if err != nil {
		m.log.Error("Error while modeling index for accounts",
			"Error", err)
		return err
	}
	return nil
}

func (m *MongoDBProcessor) CreateBalanceChangeLogsIndexes(found bool) error {
	m.balanceChangeLogsCollection = m.database.Collection("balanceChangeLogs")
	if found {
		return nil
	}
	_, err := m.balanceChangeLogsCollection.Indexes().CreateMany(context.Background(),
		[]mongo.IndexModel{
			{Keys: bson.M{"blockNumber": int32(-1)}},
		})
	if err != nil {
		m.log.Error("Error while modeling index for balanceChangeLogs",
			"Error", err)
		return err
	}
	return nil
}

func (m *MongoDBProcessor) CreateIndexes() error {
	collectionsLists := map[string]interface{}{
		"blocks":            m.CreateBlocksIndexes,
		"accounts":          m.CreateAccountsIndexes,
		"balanceChangeLogs": m.CreateBalanceChangeLogsIndexes,
	}

	for collectionName, indexCreatorFunc := range collectionsLists {
		found, err := m.IsCollectionExists(collectionName)
		if err != nil {
			return err
		}
		err = indexCreatorFunc.(func(bool) error)(found)
		if err != nil {
			return err
		}
	}

	return nil
}

func CreateMongoDBProcessor() (*MongoDBProcessor, error) {
	m := &MongoDBProcessor{}
	m.log = log.GetLogger()
	m.config = config.GetConfig()

	mongoDBConfig := m.config.GetMongoDBConfig()
	dbName := mongoDBConfig.DBName
	host := mongoDBConfig.Host
	port := mongoDBConfig.Port
	username := mongoDBConfig.Username
	password := mongoDBConfig.Password

	m.ctx, _ = context.WithTimeout(context.Background(), 60*time.Second)
	mongoURL := fmt.Sprintf("mongodb://%s:%d", host, port)
	if len(username) > 0 {
		mongoURL = fmt.Sprintf(
			"mongodb://%s:%s@%s:%d/%s",
			username, password, host, port, dbName)
	}
	clientOptions := options.Client().ApplyURI(mongoURL)
	client, err := mongo.NewClient(clientOptions)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	err = client.Connect(m.ctx)
	if err != nil {
		fmt.Println(err)
		return nil, err
	}
	m.ctx = context.TODO()
	m.client = client
	m.database = m.client.Database(dbName)
	err = m.CreateIndexes()
	if err != nil {
		return nil, err
	}

	return m, nil
}
