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
	ProjectId       string `yaml:"projectId"`
	DatasetId       string `yaml:"datasetId"`
	TableId         string `yaml:"tableId"`
	CredentialsFile string `yaml:"credentialsFile"`
}

type BulkConfiguration struct {
	MaxBatchSize               int           `yaml:"maxBatchSize"`
	MaxBufferSize              int           `yaml:"maxBufferSize"`
	BatchTickerDuration        time.Duration `yaml:"batchTickerDuration"`
	BufferFlushTickerDuration  time.Duration `yaml:"bufferFlushTickerDuration"`
	QueryExecuteTickerDuration time.Duration `yaml:"queryExecuteTickerDuration"`
	QueryExecuteThresholdCount int           `yaml:"queryExecuteThresholdCount"`
}

type Connector struct {
	BigQuery BigQuery          `yaml:"bigQuery" mapstructure:"bigQuery"`
	Bulk     BulkConfiguration `yaml:"bulk" mapstructure:"bulk"`
	Dcp      config.Dcp        `yaml:",inline" mapstructure:",squash"`
}

func (c *Connector) ApplyDefaults() {
	if c.Bulk.BatchTickerDuration.Nanoseconds() == 0 {
		c.Bulk.BatchTickerDuration = 10 * time.Second
	}
	if c.Bulk.MaxBatchSize == 0 {
		c.Bulk.MaxBatchSize = 250
	}
	if c.Bulk.MaxBufferSize == 0 {
		c.Bulk.MaxBufferSize = 500
	}
	if c.Bulk.BufferFlushTickerDuration.Nanoseconds() == 0 {
		c.Bulk.BufferFlushTickerDuration = 20 * time.Second
	}
	if c.Bulk.QueryExecuteTickerDuration.Nanoseconds() == 0 {
		c.Bulk.QueryExecuteTickerDuration = 10 * time.Minute
	}
	if c.Bulk.QueryExecuteThresholdCount == 0 {
		c.Bulk.QueryExecuteThresholdCount = 10_000
	}
}
