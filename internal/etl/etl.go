package etl

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	"elt-pipeline/internal/database"
	"elt-pipeline/internal/models"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

const batchSize = 5000
const sourceTable = "Sales"

func Run(source *sql.DB, target *pgxpool.Pool, metrics *models.Metric) {
	batchChan := make(chan []models.DataRow, 5)
	var wg sync.WaitGroup

	// Extractor
	wg.Add(1)
	go func() {
		defer wg.Done()
		start := time.Now()
		if err := extract(source, batchChan, metrics); err != nil {
			log.Printf("Extraction Error: %v", err)
			metrics.ErrorMessage = fmt.Sprintf("Extract failed: %v", err)
			metrics.ErrorCount++
		}
		metrics.ExtractionTime = time.Since(start)
		close(batchChan)
	}()

	// Loader
	start := time.Now()
	if err := load(target, batchChan, metrics); err != nil {
		log.Printf("Load Error: %v", err)
		metrics.ErrorMessage = fmt.Sprintf("Load failed: %v", err)
		metrics.ErrorCount++
	} else {
		metrics.Status = "SUCCESS"
	}
	metrics.LoadTime = time.Since(start)

	wg.Wait()
}

func extract(db *sql.DB, out chan<- []models.DataRow, metrics *models.Metric) error {
	query := fmt.Sprintf(`
        SELECT fsno, salestype, attachmentno, customer, region, date, code, name, measurementunit, unitprice, soldquantity, netpay 
        FROM %s`, sourceTable)

	rows, err := db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	batch := make([]models.DataRow, 0, batchSize)

	for rows.Next() {
		var r models.DataRow
		var dateStr sql.NullString
		var up, sq, np sql.NullFloat64
		var fs, st, an, cu, re, co, na, mu sql.NullString

		if err := rows.Scan(&fs, &st, &an, &cu, &re, &dateStr, &co, &na, &mu, &up, &sq, &np); err != nil {
			metrics.ErrorCount++
			continue
		}

		r.FsNo, r.SaleType, r.AttachmentNo = fs.String, st.String, an.String
		r.Customer, r.Region, r.Code = cu.String, re.String, co.String
		r.Name, r.MeasurementUnit = na.String, mu.String
		r.UnitPrice, r.SoldQuantity, r.NetPay = up.Float64, sq.Float64, np.Float64

		if dateStr.Valid && dateStr.String != "" {
			if t, err := time.Parse("1/2/2006", dateStr.String); err == nil {
				r.Date = t
			}
		}

		batch = append(batch, r)
		if len(batch) >= batchSize {
			out <- batch
			batch = make([]models.DataRow, 0, batchSize)
		}
	}
	if len(batch) > 0 {
		out <- batch
	}
	return rows.Err()
}

func load(pool *pgxpool.Pool, in <-chan []models.DataRow, metrics *models.Metric) error {
	for batch := range in {
		rows := make([][]interface{}, len(batch))
		for i, r := range batch {
			rows[i] = []interface{}{
				r.FsNo, r.SaleType, r.AttachmentNo, r.Customer, r.Region, r.Date,
				r.Code, r.Name, r.MeasurementUnit, r.UnitPrice, r.SoldQuantity, r.NetPay,
			}
		}

		count, err := pool.CopyFrom(
			context.Background(),
			pgx.Identifier{database.TargetTableName},
			[]string{"fsno", "salestype", "attachmentno", "customer", "region", "date", "code", "name", "measurementunit", "unitprice", "soldquantity", "netpay"},
			pgx.CopyFromRows(rows),
		)
		if err != nil {
			return err
		}
		metrics.BatchesProcessed++
		metrics.TotalRowsMigrated += int(count)
	}
	return nil
}
