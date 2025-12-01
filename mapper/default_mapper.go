package mapper

import (
	"encoding/json"
	"fmt"

	"github.com/Trendyol/go-dcp-bq/bigquery"
	"github.com/Trendyol/go-dcp-bq/config"
	"github.com/Trendyol/go-dcp-bq/couchbase"
)

// defaultMapper provides a simple 1:1 mapping implementation.
// It transforms Couchbase events to BigQuery rows without any field mapping or transformation,
// preserving the original document structure.
type defaultMapper struct{}

func NewDefaultMapper() Service {
	return &defaultMapper{}
}

func (dm *defaultMapper) TransformEvents(events []couchbase.Event) ([]bigquery.Row, error) {
	results := make([]bigquery.Row, 0, len(events))

	for _, event := range events {
		eventRows, err := dm.processEventValue(event)
		if err != nil {
			return nil, fmt.Errorf("failed to process event with key '%s': %w", string(event.Key), err)
		}
		results = append(results, eventRows...)
	}

	return results, nil
}

func (dm *defaultMapper) processEventValue(event couchbase.Event) ([]bigquery.Row, error) {
	var singleData map[string]any
	if err := json.Unmarshal(event.Value, &singleData); err == nil {
		return dm.createRowsFromData(singleData, event), nil
	}

	var arrayData []map[string]any
	if err := json.Unmarshal(event.Value, &arrayData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event value as object or array: %w", err)
	}

	results := make([]bigquery.Row, 0, len(arrayData))
	for _, itemData := range arrayData {
		itemRows := dm.createRowsFromData(itemData, event)
		results = append(results, itemRows...)
	}

	return results, nil
}

func (dm *defaultMapper) createRowsFromData(data map[string]any, event couchbase.Event) []bigquery.Row {
	row := bigquery.NewRow(data, bigquery.OperationUpsert)
	row.Data[config.CBKey] = string(event.Key)
	row.EventTime = event.EventTime
	return []bigquery.Row{*row}
}
