package main

import (
	"encoding/json"
	"fmt"

	dcpbq "github.com/Trendyol/go-dcp-bq"
	"github.com/Trendyol/go-dcp-bq/bigquery"
	"github.com/Trendyol/go-dcp-bq/couchbase"
	"github.com/Trendyol/go-dcp-bq/mapper"
)

func main() {
	connector, err := dcpbq.NewConnectorBuilder("config.yml").
		SetMapper(NewHashKeyUnwrapMapper()).
		SetBigqueryAPIVersion(bigquery.V1).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}

type hashKeyUnwrapMapper struct{}

func NewHashKeyUnwrapMapper() mapper.Service {
	return &hashKeyUnwrapMapper{}
}

func (m *hashKeyUnwrapMapper) TransformEvents(events []couchbase.Event) ([]bigquery.Row, error) {
	results := make([]bigquery.Row, 0, len(events))

	for _, event := range events {
		rows, err := m.processEventValue(event)
		if err != nil {
			return nil, fmt.Errorf("failed processing event %s: %w", string(event.Key), err)
		}
		results = append(results, rows...)
	}

	return results, nil
}
func (m *hashKeyUnwrapMapper) processEventValue(event couchbase.Event) ([]bigquery.Row, error) {
	var root map[string]any
	if err := json.Unmarshal(event.Value, &root); err != nil {
		return nil, fmt.Errorf("invalid JSON: %w", err)
	}

	if len(root) == 0 {
		return nil, fmt.Errorf("empty JSON document")
	}

	rootKeys := make([]string, 0, len(root))
	for k := range root {
		rootKeys = append(rootKeys, k)
	}

	payloadBytes, err := json.Marshal(root)
	if err != nil {
		return nil, fmt.Errorf("failed to marshal payload: %w", err)
	}

	row := bigquery.NewRow(
		map[string]any{
			"__cb_key":  string(event.Key),
			"root_keys": rootKeys,
			"payload":   string(payloadBytes),
		},
		bigquery.OperationUpsert,
	)

	row.EventTime = event.EventTime

	return []bigquery.Row{*row}, nil
}
