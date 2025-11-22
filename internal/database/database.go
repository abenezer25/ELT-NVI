package database

import (
	"context"
	"database/sql"
	"fmt"

	"elt-pipeline/internal/models"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const (
	TargetTableName   = "salesdb"
	MetricsCollection = "elt_metrics"
)

func ConnectMSSQL(dsn string) (*sql.DB, error) {
	return sql.Open("sqlserver", dsn)
}

func ConnectPostgres(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, err
	}
	config.MaxConns = 10
	return pgxpool.NewWithConfig(ctx, config)
}

func ConnectMongo(ctx context.Context, uri string) (*mongo.Client, error) {
	opts := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, opts)
	if err != nil {
		return nil, err
	}
	if err = client.Ping(ctx, nil); err != nil {
		return nil, err
	}
	return client, nil
}

func InitTable(ctx context.Context, pool *pgxpool.Pool) error {
	sql := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            fsno VARCHAR(50),
            salestype VARCHAR(50),
            attachmentno VARCHAR(50),
            customer VARCHAR(100),
            region VARCHAR(50),
            date TIMESTAMP,
            code VARCHAR(50),
            name VARCHAR(100),
            measurementunit VARCHAR(50),
            unitprice NUMERIC(12, 2),
            soldquantity NUMERIC(12, 2),
            netpay NUMERIC(12, 2)
        );`, TargetTableName)
	_, err := pool.Exec(ctx, sql)
	return err
}

func SaveMetrics(ctx context.Context, client *mongo.Client, dbName string, m models.Metric) error {
	coll := client.Database(dbName).Collection(MetricsCollection)
	_, err := coll.InsertOne(ctx, m)
	return err
}
