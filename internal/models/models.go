package models

import "time"

// Metric holds performance data
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

// DataRow represents the transformed data
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
