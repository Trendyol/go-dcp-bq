package bulk

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	_bq "github.com/Trendyol/go-dcp-bq/bigquery"
	"github.com/Trendyol/go-dcp-bq/bigquery/v1/client"
	"github.com/Trendyol/go-dcp-bq/config"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
)

const (
	maxConcurrentBatchUpsert = 20
)

type Bulk struct {
	bigQueryClient      client.Client
	dcpCheckpointCommit func()
	config              config.BulkConfiguration
	targetTable         _bq.Identifier

	queryExecuteTicker *time.Ticker
	queryExecuteMutex  sync.Mutex

	buffer *buffer

	isDcpRebalancing bool

	metric *_bq.Metric
}

type buffer struct {
	_bq.BatchBuffer
	accumulatedCount int
}

func New(cfg *config.Connector, dcpCheckpointCommit func()) (*Bulk, error) {
	if err := validateConfig(cfg); err != nil {
		return nil, err
	}

	bigQueryClient, err := client.NewBigQueryClient(context.Background(), cfg.BigQuery)
	if err != nil {
		return nil, err
	}

	if err := validateTargetTable(bigQueryClient, cfg.BigQuery); err != nil {
		return nil, err
	}

	sourceTable, err := bigQueryClient.CreateTemporaryTable(
		cfg.BigQuery.ProjectId,
		cfg.BigQuery.DatasetId,
		cfg.BigQuery.TableId,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create temporary table: %w", err)
	}

	bulk := &Bulk{
		bigQueryClient:      bigQueryClient,
		dcpCheckpointCommit: dcpCheckpointCommit,
		config:              cfg.BigQuery.Bulk,
		targetTable:         *_bq.NewIdentifier(cfg.BigQuery.ProjectId, cfg.BigQuery.DatasetId, cfg.BigQuery.TableId),
		queryExecuteTicker:  time.NewTicker(cfg.BigQuery.Bulk.QueryExecuteTickerDuration),
		buffer:              newBuffer(cfg.BigQuery.Bulk, sourceTable),
		metric:              &_bq.Metric{},
		isDcpRebalancing:    false,
	}

	return bulk, nil
}

func newBuffer(cfg config.BulkConfiguration, sourceTable *_bq.Identifier) *buffer {
	return &buffer{
		BatchBuffer: _bq.BatchBuffer{
			Items:       make([]_bq.Row, 0, cfg.MaxBufferSize),
			Ticker:      time.NewTicker(cfg.BufferFlushTickerDuration),
			MaxSize:     cfg.MaxBufferSize,
			Mu:          sync.Mutex{},
			SourceTable: *sourceTable,
		},
		accumulatedCount: 0,
	}
}

func validateConfig(cfg *config.Connector) error {
	if cfg.BigQuery.ProjectId == "" {
		return fmt.Errorf("project id is required")
	}
	if cfg.BigQuery.DatasetId == "" {
		return fmt.Errorf("dataset id is required")
	}
	if cfg.BigQuery.TableId == "" {
		return fmt.Errorf("table id is required")
	}
	if cfg.BigQuery.CredentialsFile == "" {
		return fmt.Errorf("credentials file is required")
	}

	return nil
}

func validateTargetTable(client client.Client, bqConfig config.BigQuery) error {
	ctx := context.Background()

	table := client.GetTable(bqConfig.ProjectId, bqConfig.DatasetId, bqConfig.TableId)
	if _, err := table.Metadata(ctx); err != nil {
		targetTable := formatTableName(bqConfig.ProjectId, bqConfig.DatasetId, bqConfig.TableId)
		return fmt.Errorf("target table '%s' does not exist: %w", targetTable, err)
	}

	columns, err := client.GetTableColumns(ctx, bqConfig.ProjectId, bqConfig.DatasetId, bqConfig.TableId)
	if err != nil {
		return fmt.Errorf("failed to get table columns for validation: %w", err)
	}

	if !containsColumn(columns, config.CBKey) {
		targetTable := formatTableName(bqConfig.ProjectId, bqConfig.DatasetId, bqConfig.TableId)
		return fmt.Errorf("target table '%s' must contain '%s' column for DCP operations", targetTable, config.CBKey)
	}

	return nil
}

func formatTableName(projectId, datasetId, tableId string) string {
	return fmt.Sprintf("%s.%s.%s", projectId, datasetId, tableId)
}

func containsColumn(columns []string, target string) bool {
	for _, column := range columns {
		if column == target {
			return true
		}
	}
	return false
}

func (b *Bulk) GetMetric() *_bq.Metric {
	return b.metric
}

func (b *Bulk) StartBulk() {
	go b.startBufferFlushLoop(b.buffer, b.flushBuffer)
	go b.startQueryExecuteLoop()
}

func (b *Bulk) startBufferFlushLoop(buf *buffer, flushFunc func()) {
	for range buf.Ticker.C {
		flushFunc()
	}
}

func (b *Bulk) startQueryExecuteLoop() {
	for range b.queryExecuteTicker.C {
		b.executeQuery()
	}
}

func (b *Bulk) AddActions(ctx *models.ListenerContext, eventTime time.Time, actions []_bq.Row) error {
	b.buffer.Mu.Lock()
	defer b.buffer.Mu.Unlock()
	if b.isDcpRebalancing {
		logger.Log.Warn("Could not add new items to batch while rebalancing")
		return fmt.Errorf("writer is rebalancing")
	}

	b.buffer.Items = append(b.buffer.Items, actions...)
	b.buffer.accumulatedCount += len(actions)

	if len(b.buffer.Items) >= b.config.MaxBufferSize {
		logger.Log.Info("Buffer full, flushing")
		b.flushBuffer()
	}

	b.metric.ProcessLatencyMs = time.Since(eventTime).Milliseconds()
	return nil
}

func (b *Bulk) flushBuffer() {
	if b.isDcpRebalancing {
		logger.Log.Warn("Could not flush batch while rebalancing")
		return
	}

	if len(b.buffer.Items) == 0 {
		return
	}

	startTime := time.Now()
	batch := b.extractBatch(b.buffer)

	b.acknowledgeBatch(batch)

	tempData := b.convertToValueSavers(batch)
	sourceTable := b.bigQueryClient.GetTable(b.buffer.SourceTable.ProjectId, b.buffer.SourceTable.DatasetId, b.buffer.SourceTable.TableName)

	b.processBatchesInParallel(sourceTable, tempData)

	if b.buffer.accumulatedCount >= b.config.QueryExecuteThresholdCount {
		if err := b.executeQuery(); err != nil {
			logger.Log.Error("error while executing query, err: %v", err)
			return
		}
	}

	b.metric.BulkRequestProcessLatencyMs = time.Since(startTime).Milliseconds()
}

func (b *Bulk) extractBatch(buf *buffer) []_bq.Row {
	batch := make([]_bq.Row, len(buf.Items))
	copy(batch, buf.Items)
	buf.Items = buf.Items[:0]
	return batch
}

func (b *Bulk) acknowledgeBatch(batch []_bq.Row) {
	for _, row := range batch {
		row.Ctx.Ack()
	}
}

func (b *Bulk) convertToValueSavers(batch []_bq.Row) []bigquery.ValueSaver {
	tempData := make([]bigquery.ValueSaver, len(batch))
	for i, row := range batch {
		tempData[i] = &row
	}
	return tempData
}

// processBatchesInParallel splits data into configurable batch sizes and processes them concurrently
func (b *Bulk) processBatchesInParallel(table *bigquery.Table, data []bigquery.ValueSaver) {
	maxBatchSize := b.config.MaxBatchSize

	if len(data) == 0 {
		return
	}

	if len(data) <= maxBatchSize {
		if err := b.bigQueryClient.Insert(context.Background(), table, data); err != nil {
			logger.Log.Error("error while writing to bigquery, err: %v", err)
		}
		return
	}

	var wg sync.WaitGroup
	sem := make(chan struct{}, maxConcurrentBatchUpsert)

	for i := 0; i < len(data); i += maxBatchSize {
		end := min(i+maxBatchSize, len(data))

		wg.Add(1)
		sem <- struct{}{}

		go func(chunk []bigquery.ValueSaver) {
			defer wg.Done()
			defer func() { <-sem }()

			if err := b.bigQueryClient.Insert(
				context.Background(),
				table,
				chunk,
			); err != nil {
				logger.Log.Error("error while writing to bigquery, err: %v", err)
				return
			}
		}(data[i:end])
	}

	wg.Wait()
}

func (b *Bulk) executeQuery() error {
	b.queryExecuteMutex.Lock()
	defer b.queryExecuteMutex.Unlock()

	logger.Log.Info("Executing query")

	columns, err := b.getFilteredColumns()
	if err != nil {
		return err
	}

	unifiedQuery := _bq.BuildQuery(
		b.targetTable.ProjectId,
		b.targetTable.DatasetId,
		b.targetTable.TableName,
		b.buffer.SourceTable.ToTableId(),
		columns,
	)

	if err := b.bigQueryClient.ExecuteQuery(context.Background(), unifiedQuery); err != nil {
		return err
	}

	if err := b.recreateTemporaryTable(b.buffer); err != nil {
		return err
	}

	b.buffer.accumulatedCount = 0
	logger.Log.Info("Executed query")

	b.dcpCheckpointCommit()
	return nil
}

func (b *Bulk) getFilteredColumns() ([]string, error) {
	columns, err := b.bigQueryClient.GetTableColumns(
		context.Background(),
		b.targetTable.ProjectId,
		b.targetTable.DatasetId,
		b.targetTable.TableName,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to get table columns: %w", err)
	}

	filteredColumns := make([]string, 0, len(columns))
	for _, column := range columns {
		if column != config.CBKey && column != config.OperationType && column != config.EventTime {
			filteredColumns = append(filteredColumns, column)
		}
	}
	return filteredColumns, nil
}

func (b *Bulk) recreateTemporaryTable(buf *buffer) error {
	id, err := b.bigQueryClient.CreateTemporaryTable(
		b.targetTable.ProjectId,
		b.targetTable.DatasetId,
		b.targetTable.TableName,
	)
	if err != nil {
		logger.Log.Error("error while creating temporary table, err: %v", err)
		panic(err)
	}
	buf.SourceTable = *id
	return nil
}

func (b *Bulk) PrepareStartRebalancing() {
	b.buffer.Mu.Lock()
	defer b.buffer.Mu.Unlock()

	b.isDcpRebalancing = true
}

func (b *Bulk) PrepareEndRebalancing() {
	b.buffer.Mu.Lock()
	defer b.buffer.Mu.Unlock()

	b.isDcpRebalancing = false
}

func (b *Bulk) Close() error {
	logger.Log.Info("Closing bigquery bulk writer")
	b.buffer.Ticker.Stop()
	b.queryExecuteTicker.Stop()

	b.buffer.Mu.Lock()
	defer b.buffer.Mu.Unlock()

	if len(b.buffer.Items) > 0 {
		b.flushBuffer()
	}

	b.queryExecuteMutex.Lock()
	defer b.queryExecuteMutex.Unlock()
	if b.buffer.accumulatedCount > 0 {
		b.executeQuery()
	}

	logger.Log.Info("BigQuery bulk writer closed successfully")
	return nil
}
