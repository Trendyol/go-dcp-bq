package mapper

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Trendyol/go-dcp-bq/bigquery"
	"github.com/Trendyol/go-dcp-bq/config"
	"github.com/Trendyol/go-dcp-bq/couchbase"
)

// Service defines the contract for transforming Couchbase DCP events into BigQuery rows.
// Implementations should handle the conversion of Couchbase document events to BigQuery-compatible
// data structures, applying any necessary data transformations, mappings, or filtering logic.
type Service interface {
	TransformEvents(events []couchbase.Event) ([]bigquery.Row, error)
}

type Config struct {
	Mappings map[string]string // "source.path" -> "target_field"
}

type service struct {
	config *Config
}

func New(config *Config) *service {
	return &service{
		config: config,
	}
}

func (s *service) TransformEvents(events []couchbase.Event) ([]bigquery.Row, error) {
	results := make([]bigquery.Row, 0, len(events))

	for _, event := range events {
		eventRows, err := s.processEventValue(event)
		if err != nil {
			return nil, fmt.Errorf("failed to process event with key '%s': %w", string(event.Key), err)
		}
		results = append(results, eventRows...)
	}

	return results, nil
}

func (s *service) processEventValue(event couchbase.Event) ([]bigquery.Row, error) {
	var singleData map[string]any
	if err := json.Unmarshal(event.Value, &singleData); err == nil {
		return s.transformSingleEvent(singleData, event)
	}

	var arrayData []map[string]any
	if err := json.Unmarshal(event.Value, &arrayData); err != nil {
		return nil, fmt.Errorf("failed to unmarshal event value as object or array: %w", err)
	}

	results := make([]bigquery.Row, 0, len(arrayData))
	for i, itemData := range arrayData {
		itemRows, err := s.transformSingleEvent(itemData, event)
		if err != nil {
			return nil, fmt.Errorf("failed to process array item at index %d: %w", i, err)
		}
		results = append(results, itemRows...)
	}

	return results, nil
}

// transformSingleEvent transforms a single data object to BigQuery rows
func (s *service) transformSingleEvent(data map[string]any, event couchbase.Event) ([]bigquery.Row, error) {
	transformed, err := s.transform(data)
	if err != nil {
		return nil, fmt.Errorf("transform failed: %w", err)
	}
	transformed.Data[config.CBKey] = string(event.Key)
	return []bigquery.Row{*transformed}, nil
}

func (s *service) transform(doc map[string]any) (*bigquery.Row, error) {
	result := make(map[string]any)

	for sourcePath, targetField := range s.config.Mappings {
		value, err := getNestedValue(doc, sourcePath)
		if err != nil {
			continue
		}

		setNestedValue(result, targetField, value)
	}

	return bigquery.NewRow(result, bigquery.OperationUpsert), nil
}

func getNestedValue(doc map[string]any, path string) (any, error) {
	if path == "" {
		return nil, fmt.Errorf("path cannot be empty")
	}

	parts := strings.Split(path, ".")
	current := interface{}(doc)

	for _, part := range parts {
		switch v := current.(type) {
		case map[string]any:
			value, ok := v[part]
			if !ok {
				return nil, fmt.Errorf("field not found: %s", path)
			}
			current = value
		default:
			return nil, fmt.Errorf("expected object at path: %s", path)
		}
	}

	return current, nil
}

func setNestedValue(doc map[string]any, path string, value any) error {
	if path == "" {
		return fmt.Errorf("path cannot be empty")
	}

	parts := strings.Split(path, ".")
	current := doc

	for i, part := range parts {
		if i == len(parts)-1 {
			current[part] = value
		} else {
			if _, ok := current[part]; !ok {
				current[part] = make(map[string]any)
			}

			next, ok := current[part].(map[string]any)
			if !ok {
				return fmt.Errorf("cannot set nested value: %s is not a map", part)
			}
			current = next
		}
	}

	return nil
}
