package config

import (
	"fmt"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	MSSQLDSN    string
	PostgresDSN string
	MongoURI    string
	MongoDBName string
}

func Load() (*Config, error) {
	// Load .env file if exists
	_ = godotenv.Load()

	cfg := &Config{
		MSSQLDSN:    os.Getenv("MSSQL_CONN"),
		PostgresDSN: os.Getenv("POSTGRES_CONN"),
		MongoURI:    os.Getenv("MONGO_URI"),
		MongoDBName: os.Getenv("MONGO_DB_NAME"),
	}

	if cfg.MSSQLDSN == "" || cfg.PostgresDSN == "" || cfg.MongoURI == "" {
		return nil, fmt.Errorf("missing required environment variables")
	}

	return cfg, nil
}
