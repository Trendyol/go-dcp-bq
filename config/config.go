package config

import (
	"time"

	"github.com/Trendyol/go-dcp/config"
)

const (
	CBKey         = "__cb_key"
	OperationType = "__operation_type"
	EventTime     = "__event_time"
)

type BigQuery struct {
	ProjectId       string            `yaml:"projectId"`
	DatasetId       string            `yaml:"datasetId"`
	TableId         string            `yaml:"tableId"`
	CredentialsFile string            `yaml:"credentialsFile"`
	Bulk            BulkConfiguration `yaml:"bulk" mapstructure:"bulk"`
}

type BulkConfiguration struct {
	MaxBatchSize               int           `yaml:"maxBatchSize"`
	MaxBufferSize              int           `yaml:"maxBufferSize"`
	BufferFlushTickerDuration  time.Duration `yaml:"bufferFlushTickerDuration"`
	QueryExecuteTickerDuration time.Duration `yaml:"queryExecuteTickerDuration"`
	QueryExecuteThresholdCount int           `yaml:"queryExecuteThresholdCount"`
}

type Connector struct {
	BigQuery BigQuery   `yaml:"bigQuery" mapstructure:"bigQuery"`
	Dcp      config.Dcp `yaml:",inline" mapstructure:",squash"`
}

func (c *Connector) ApplyDefaults() {
	if c.BigQuery.Bulk.MaxBatchSize <= 0 {
		c.BigQuery.Bulk.MaxBatchSize = 500
	}
	if c.BigQuery.Bulk.MaxBufferSize <= 0 {
		c.BigQuery.Bulk.MaxBufferSize = 25_000
	}
	if c.BigQuery.Bulk.BufferFlushTickerDuration.Nanoseconds() <= 0 {
		c.BigQuery.Bulk.BufferFlushTickerDuration = 2 * time.Minute
	}
	if c.BigQuery.Bulk.QueryExecuteTickerDuration.Nanoseconds() <= 0 {
		c.BigQuery.Bulk.QueryExecuteTickerDuration = 10 * time.Minute
	}
	if c.BigQuery.Bulk.QueryExecuteThresholdCount <= 0 {
		c.BigQuery.Bulk.QueryExecuteThresholdCount = 250_000
	}
}
