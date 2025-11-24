package config

import (
	"fmt"
	"log"
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

	envPaths := []string{".env", "../.env", "../../.env"}
	loaded := false
	for _, p := range envPaths {
		if err := godotenv.Load(p); err == nil {
			log.Printf("Loaded env from %s", p)
			loaded = true
			break
		}
	}
	if !loaded {
		log.Printf("No .env file found in %v; relying on environment variables", envPaths)
	}

	cfg := &Config{
		MSSQLDSN:    os.Getenv("MSSQL_CONN"),
		PostgresDSN: os.Getenv("POSTGRES_CONN"),
		MongoURI:    os.Getenv("MONGO_URI"),
		MongoDBName: os.Getenv("MONGO_DB_NAME"),
	}

	missing := []string{}
	if cfg.MSSQLDSN == "" {
		missing = append(missing, "MSSQL_CONN")
	}
	if cfg.PostgresDSN == "" {
		missing = append(missing, "POSTGRES_CONN")
	}
	if cfg.MongoURI == "" {
		missing = append(missing, "MONGO_URI")
	}
	if cfg.MongoDBName == "" {
		missing = append(missing, "MONGO_DB_NAME")
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("missing required environment variables: %v", missing)
	}

	return cfg, nil
}
