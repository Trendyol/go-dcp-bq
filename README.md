# Go DCP BigQuery [![Go Reference](https://pkg.go.dev/badge/github.com/Trendyol/go-dcp-bq.svg)](https://pkg.go.dev/github.com/Trendyol/go-dcp-bq) [![Go Report Card](https://goreportcard.com/badge/github.com/Trendyol/go-dcp-bq)](https://goreportcard.com/report/github.com/Trendyol/go-dcp-bq) [![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/Trendyol/go-dcp-bq/badge)](https://scorecard.dev/viewer/?uri=github.com/Trendyol/go-dcp-bq)

**Go DCP BigQuery** streams documents from Couchbase Database Change Protocol (DCP) and writes to
Google BigQuery table.

## ⚠️ Important Notice - Beta Version

**This connector is currently in BETA and has important limitations:**

- **❌ NO HORIZONTAL SCALING**: The connector does **NOT** support running multiple instances/pods simultaneously
- **⚠️ SINGLE INSTANCE ONLY**: You **MUST** run exactly **ONE** instance at a time to prevent:
  - Data inconsistency
  - Duplicate writes
  - Data corruption

## Features

* **Automatic data type mapping** from Couchbase JSON to BigQuery schema
* Custom **mappers** for transforming DCP events to BigQuery rows
* **Bulk insert** operations for high throughput
* Handling different DCP events such as **expiration, deletion and mutation**
* Built on top of [go-dcp](https://github.com/Trendyol/go-dcp) for reliable DCP streaming
* **Metric collection** for monitoring and observability
* **Configurable deployment** with YAML-based configuration

## Table of Contents

- [Installation](#installation)
- [Quick Start](#quick-start)
- [Examples](#examples)
  - [Default Mapper](#default-mapper-example)
  - [Simple Mapper with Field Mappings](#simple-mapper-example)
  - [Custom Mapper](#custom-mapper-example)
- [Configuration](#configuration)
- [Running the Connector](#running-the-connector)
- [BigQuery Table Schema](#bigquery-table-schema)
- [Exposed Metrics](#exposed-metrics)
- [Contributing](#contributing)
- [License](#license)

## Installation

```bash
go get github.com/Trendyol/go-dcp-bq
```

## Quick Start

1. Create a BigQuery table with required columns (`__cb_key`)
2. Set up Google Cloud credentials
3. Create a configuration file (`config.yml`)
4. Choose your mapper strategy (default, simple, or custom)
5. Run your connector

## Examples

### Default Mapper Example

The simplest way to get started. Uses default mapper that writes entire document to BigQuery.

```go
package main

import (
	dcpbq "github.com/Trendyol/go-dcp-bq"
	"github.com/Trendyol/go-dcp-bq/bigquery"
	"github.com/Trendyol/go-dcp-bq/mapper"
)

func main() {
	connector, err := dcpbq.NewConnectorBuilder("config.yml").
		SetMapper(mapper.NewDefaultMapper()).
		SetBigqueryAPIVersion(bigquery.V1).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
```

### Simple Mapper Example

Map specific JSON fields to BigQuery columns with field path mapping.

```go
package main

import (
	dcpbq "github.com/Trendyol/go-dcp-bq"
	"github.com/Trendyol/go-dcp-bq/bigquery"
	"github.com/Trendyol/go-dcp-bq/mapper"
)

func main() {
	connector, err := dcpbq.NewConnectorBuilder("config.yml").
		SetMapper(mapper.New(
			&mapper.Config{
				Mappings: map[string]string{
					"user.id":                      "user_id",
					"user.name":                    "user_name",
					"user.email":                   "user_email",
					"user.address.aparment.number": "user_address_aparment_number",
				},
			},
		)).
		SetBigqueryAPIVersion(bigquery.V1).
		Build()
	if err != nil {
		panic(err)
	}

	defer connector.Close()
	connector.Start()
}
```

### Custom Mapper Example

Full control over event transformation with custom logic.

```go
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
```

### BigQuery Specific Configuration

| Variable                                    | Type          | Required | Default | Description                                                                                        |
|---------------------------------------------|---------------|----------|---------|----------------------------------------------------------------------------------------------------| 
| `bigQuery.projectId`                        | string        | yes      |         | Google Cloud Project ID                                                                            |
| `bigQuery.datasetId`                        | string        | yes      |         | BigQuery Dataset ID                                                                                |
| `bigQuery.tableId`                          | string        | yes      |         | BigQuery Table ID                                                                                  |
| `bigQuery.credentialsFile`                  | string        | yes      |         | Path to Google Cloud service account JSON credentials file                                         |
| `bigQuery.bulk.maxBatchSize`                | int           | no       | 500     | Maximum number of rows to insert in a single batch operation                                       |
| `bigQuery.bulk.maxBufferSize`               | int           | no       | 25000    | Maximum buffer size before triggering a flush                                                      |
| `bigQuery.bulk.bufferFlushTickerDuration`   | time.Duration | no       | 2m     | Time interval for automatic buffer flush                                                           |
| `bigQuery.bulk.queryExecuteTickerDuration`  | time.Duration | no       | 10m     | Time interval for automatic query execution                                                        |
| `bigQuery.bulk.queryExecuteThresholdCount`  | int           | no       | 250000   | Number of accumulated records that triggers query execution                                        |

## Contributing

Go DCP BigQuery is always open for direct contributions. For more information please check
our [Contribution Guideline document](./CONTRIBUTING.md).

## License

Released under the [MIT License](LICENSE).
