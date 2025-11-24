package main

import (
	"context"
	"log"
	"time"

	"elt-pipeline/internal/config"
	"elt-pipeline/internal/database"
	"elt-pipeline/internal/etl"
	"elt-pipeline/internal/models"
)

func main() {
	log.Println("Starting ELT pipeline ...")

	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config load: %v", err)
	}

	metric := &models.Metric{StartTime: time.Now(), Status: "FAILURE"}

	ctx := context.Background()

	// Mongo
	mongoClient, err := database.ConnectMongo(ctx, cfg.MongoURI)
	if err != nil {
		log.Fatalf("connect mongo: %v", err)
	}
	defer mongoClient.Disconnect(ctx)

	// Source MSSQL
	sourceDB, err := database.ConnectMSSQL(cfg.MSSQLDSN)
	if err != nil {
		log.Fatalf("connect mssql: %v", err)
	}
	defer sourceDB.Close()

	// Target Postgres
	pgPool, err := database.ConnectPostgres(ctx, cfg.PostgresDSN)
	if err != nil {
		log.Fatalf("connect postgres: %v", err)
	}
	defer pgPool.Close()

	if err := database.InitTable(ctx, pgPool); err != nil {
		log.Fatalf("init table: %v", err)
	}

	// Run ETL
	etl.Run(sourceDB, pgPool, metric)

	// finalize metrics and save
	metric.EndTime = time.Now()
	metric.TotalDuration = metric.EndTime.Sub(metric.StartTime)
	if err := database.SaveMetrics(ctx, mongoClient, cfg.MongoDBName, *metric); err != nil {
		log.Printf("failed saving metrics: %v", err)
	}

	log.Printf("Finished. Rows migrated: %d, Duration: %v", metric.TotalRowsMigrated, metric.TotalDuration)
}
