package config

type Config struct {
	qrlNodeConfig *QRLNodeConfig
	mongoDBConfig *MongoDBConfig

	ReOrgLimit           uint64
	BanStartBlockNumber  uint64
	BannedQRLAddressList map[string]bool
}

type QRLNodeConfig struct {
	IP            string
	PublicAPIPort uint16
}

type MongoDBConfig struct {
	DBName   string
	Host     string
	Port     uint16
	Username string
	Password string
}

func GetConfig() *Config {
	c := &Config{
		qrlNodeConfig: &QRLNodeConfig{
			IP:            "127.0.0.1", // IP address of Python QRL node with PublicAPI support
			PublicAPIPort: 19009,
		},
		mongoDBConfig: &MongoDBConfig{
			DBName:   "QRLRichListIndexer",
			Host:     "127.0.0.1",
			Port:     27017, // Default MongoDB port
			Username: "",
			Password: "",
		},
		ReOrgLimit:          350,
		BanStartBlockNumber: 2078800,
		BannedQRLAddressList: map[string]bool{
			"Q010600fcd0db869d2e1b17b452bdf9848f6fe8c74ee5b8f935408cc558c601fb69eb553fa916a1": true,
		},
	}
	return c
}

func (c *Config) GetQRLNodeConfig() *QRLNodeConfig {
	return c.qrlNodeConfig
}

func (c *Config) GetMongoDBConfig() *MongoDBConfig {
	return c.mongoDBConfig
}
