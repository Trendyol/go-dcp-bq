package bigquery

import (
	"time"

	"cloud.google.com/go/bigquery"

	"github.com/Trendyol/go-dcp-bq/config"
	"github.com/Trendyol/go-dcp/models"
)

type OperationType string

const (
	OperationUpsert OperationType = "UPSERT"
	OperationDelete OperationType = "DELETE"
)

type Row struct {
	Ctx           *models.ListenerContext
	Data          map[string]any
	OperationType OperationType
	EventTime     time.Time
}

func NewRow(data map[string]any, opType OperationType) *Row {
	return &Row{
		Data:          data,
		OperationType: opType,
	}
}

// Save implements the ValueSaver interface for BigQuery
func (row *Row) Save() (map[string]bigquery.Value, string, error) {
	values := make(map[string]bigquery.Value)

	for key, value := range row.Data {
		values[key] = value
	}
	values[config.OperationType] = string(row.OperationType)
	values[config.EventTime] = row.EventTime

	return values, "", nil
}
