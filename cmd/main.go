package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const batchSize = 5000

const (
	sourceTableName   = "Sales"
	targetTableName   = "salesdb"
	metricsCollection = "elt_metrics"
)

type Metric struct {
	StartTime         time.Time     `bson:"startTime"`
	EndTime           time.Time     `bson:"endTime"`
	TotalDuration     time.Duration `bson:"totalDuration"`
	ExtractionTime    time.Duration `bson:"extractionTime"` 
	LoadTime          time.Duration `bson:"loadTime"`      
	Status            string        `bson:"status"`
	TotalRowsMigrated int           `bson:"totalRowsMigrated"`
	BatchesProcessed  int           `bson:"batchesProcessed"`
	ErrorCount        int           `bson:"errorCount"`
	ErrorMessage      string        `bson:"errorMessage,omitempty"`
}

type DataRow struct {
	FsNo            string
	SaleType        string
	AttachmentNo    string
	Customer        string
	Region          string
	Date            time.Time 
	Code            string
	Name            string
	MeasurementUnit string
	UnitPrice       float64
	SoldQuantity    float64
	NetPay          float64
}

func main() {
	log.Println("Starting Optimized Go ELT Pipeline...")

	metrics := &Metric{
		StartTime: time.Now(),
		Status:    "FAILURE",
	}

	// Load Config
	if err := godotenv.Load(); err != nil && !os.IsNotExist(err) {
		log.Fatalf("Error loading .env file: %v", err)
	}
	mssqlDSN := os.Getenv("MSSQL_CONN")
	postgresDSN := os.Getenv("POSTGRES_CONN")
	mongoURI := os.Getenv("MONGO_URI")
	mongoDBName := os.Getenv("MONGO_DB_NAME")

	if mssqlDSN == "" || postgresDSN == "" || mongoURI == "" || mongoDBName == "" {
		log.Fatal("Environment variables missing.")
	}

	// Connect MongoDB
	mongoClient, err := connectMongo(mongoURI, mongoDBName)
	if err != nil {
		log.Fatalf("Mongo Error: %v", err)
	}
	defer mongoClient.Disconnect(context.TODO())

	// Connect MSSQL (Source)
	sourceDB, err := sql.Open("sqlserver", mssqlDSN)
	if err != nil {
		log.Fatalf("MSSQL Error: %v", err)
	}
	defer sourceDB.Close()

	// Connect PostgreSQL 
	pgConfig, err := pgxpool.ParseConfig(postgresDSN)
	if err != nil {
		log.Fatalf("Postgres Config Error: %v", err)
	}
	pgConfig.MaxConns = 10 // Allow parallel connections if needed

	targetPool, err := pgxpool.NewWithConfig(context.Background(), pgConfig)
	if err != nil {
		log.Fatalf("Postgres Connection Error: %v", err)
	}
	defer targetPool.Close()

	if err := ensureTargetTable(targetPool); err != nil {
		log.Fatalf("Table Init Error: %v", err)
	}

	runPipeline(sourceDB, targetPool, mongoClient, mongoDBName, metrics)
}

func runPipeline(source *sql.DB, target *pgxpool.Pool, mClient *mongo.Client, mDBName string, metrics *Metric) {
	defer func() {
		metrics.EndTime = time.Now()
		metrics.TotalDuration = metrics.EndTime.Sub(metrics.StartTime)
		_ = storeMetrics(mClient, mDBName, *metrics)
		log.Printf("Pipeline Finished. Rows: %d, Duration: %v", metrics.TotalRowsMigrated, metrics.TotalDuration)
	}()

	batchChan := make(chan []DataRow, 5)
	var wg sync.WaitGroup

	// WORKER 1: EXTRACTOR 
	wg.Add(1)
	go func() {
		defer wg.Done()
		extractStart := time.Now()

		err := extractAndBatch(source, batchChan, metrics)
		if err != nil {
			log.Printf("Extraction Error: %v", err)
			metrics.ErrorMessage = fmt.Sprintf("Extract failed: %v", err)
			metrics.ErrorCount++
		}

		metrics.ExtractionTime = time.Since(extractStart)
		close(batchChan) 
	}()

	// WORKER 2: LOADER 
	loadStart := time.Now()
	err := loadBatches(target, batchChan, metrics)
	if err != nil {
		log.Printf("Load Error: %v", err)
		metrics.ErrorMessage = fmt.Sprintf("Load failed: %v", err)
		metrics.ErrorCount++
	} else {
		metrics.Status = "SUCCESS"
	}
	metrics.LoadTime = time.Since(loadStart)

	wg.Wait() 

func extractAndBatch(db *sql.DB, outChan chan<- []DataRow, metrics *Metric) error {

	query := fmt.Sprintf(`
        SELECT fsno, salestype, attachmentno, customer, region, date, code, name, measurementunit, unitprice, soldquantity, netpay
        FROM %s`, sourceTableName)

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]DataRow, 0, batchSize)

	for rows.Next() {
		var r DataRow
		var dateStr sql.NullString
		var up, sq, np sql.NullFloat64
		var fs, st, an, cu, re, co, na, mu sql.NullString

		if err := rows.Scan(&fs, &st, &an, &cu, &re, &dateStr, &co, &na, &mu, &up, &sq, &np); err != nil {
			log.Printf("Scan error: %v", err)
			metrics.ErrorCount++
			continue
		}

		// Transform
		r.FsNo = fs.String
		r.SaleType = st.String
		r.AttachmentNo = an.String
		r.Customer = cu.String
		r.Region = re.String
		r.Code = co.String
		r.Name = na.String
		r.MeasurementUnit = mu.String
		r.UnitPrice = up.Float64
		r.SoldQuantity = sq.Float64
		r.NetPay = np.Float64

		if dateStr.Valid && dateStr.String != "" {
			t, err := time.Parse("1/2/2006", dateStr.String)
			if err == nil {
				r.Date = t
			}
		}

		batch = append(batch, r)

		if len(batch) >= batchSize {
			outChan <- batch
			batch = make([]DataRow, 0, batchSize)
		}
	}

	if len(batch) > 0 {
		outChan <- batch
	}

	return rows.Err()
}

func loadBatches(pool *pgxpool.Pool, inChan <-chan []DataRow, metrics *Metric) error {
	ctx := context.Background()

	for batch := range inChan {
		start := time.Now()

		rows := make([][]interface{}, len(batch))
		for i, row := range batch {
			rows[i] = []interface{}{
				row.FsNo, row.SaleType, row.AttachmentNo, row.Customer, row.Region,
				row.Date, row.Code, row.Name, row.MeasurementUnit,
				row.UnitPrice, row.SoldQuantity, row.NetPay,
			}
		}

		count, err := pool.CopyFrom(
			ctx,
			pgx.Identifier{targetTableName},
			[]string{"fsno", "salestype", "attachmentno", "customer", "region", "date", "code", "name", "measurementunit", "unitprice", "soldquantity", "netpay"},
			pgx.CopyFromRows(rows),
		)

		if err != nil {
			log.Printf("Failed to Copy batch: %v", err)
			metrics.ErrorCount += len(batch)
			return fmt.Errorf("bulk load failed: %w", err)
		}

		metrics.BatchesProcessed++
		metrics.TotalRowsMigrated += int(count)

		if metrics.BatchesProcessed%10 == 0 {
			log.Printf("Loaded %d rows (Batch time: %v)", metrics.TotalRowsMigrated, time.Since(start))
		}
	}
	return nil
}

// Helpers 

func ensureTargetTable(pool *pgxpool.Pool) error {
	createTableSQL := fmt.Sprintf(`
        CREATE TABLE IF NOT EXISTS %s (
            fsno VARCHAR(50),
            salestype VARCHAR(50),
            attachmentno VARCHAR(50),
            customer VARCHAR(100),
            region VARCHAR(50),
            date TIMESTAMP, -- Changed from DATE to TIMESTAMP for Go time.Time compatibility
            code VARCHAR(50),
            name VARCHAR(100),
            measurementunit VARCHAR(50),
            unitprice NUMERIC(12, 2),
            soldquantity NUMERIC(12, 2),
            netpay NUMERIC(12, 2)
        );
    `, targetTableName)

	_, err := pool.Exec(context.Background(), createTableSQL)
	return err
}

func connectMongo(uri, dbName string) (*mongo.Client, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	clientOptions := options.Client().ApplyURI(uri)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return nil, err
	}
	if err = client.Ping(ctx, nil); err != nil {
		return nil, err
	}
	return client, nil
}

func storeMetrics(client *mongo.Client, dbName string, metrics Metric) error {
	collection := client.Database(dbName).Collection(metricsCollection)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := collection.InsertOne(ctx, metrics)
	return err
}
