package main

import (
	"os"
	"os/signal"

	"github.com/theQRL/qrl-rich-list-indexer/client"
	"github.com/theQRL/qrl-rich-list-indexer/db"
	"github.com/theQRL/qrl-rich-list-indexer/log"
)

func run() error {
	// Create MongoDB Processor
	m, err := db.CreateMongoDBProcessor()
	if err != nil {
		return err
	}

	nc, err := client.ConnectServer(m)
	if err != nil {
		return err
	}
	go nc.Start()
	defer nc.Stop()
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt)
	<-quit
	return nil
}

func start() {
	logger := log.GetLogger()

	err := run()
	if err != nil {
		logger.Error("Error while starting Indexer",
			"Error", err.Error())
		return
	}
}

func main() {
	logger := log.GetLogger()
	logger.Info("Starting Indexer")

	start()

	logger.Info("Shutting Down Indexer")
}
