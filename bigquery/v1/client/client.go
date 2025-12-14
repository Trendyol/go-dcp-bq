package client

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"time"

	"cloud.google.com/go/bigquery"
	_bq "github.com/Trendyol/go-dcp-bq/bigquery"
	"github.com/Trendyol/go-dcp-bq/config"
	"github.com/google/uuid"
	"google.golang.org/api/option"
)

type Client interface {
	// Insert appends the data to the given table.
	Insert(ctx context.Context, table *bigquery.Table, data any) error
	// BatchInsert inserts data into a table in chunks.
	BatchInsert(ctx context.Context, table *bigquery.Table, data []bigquery.ValueSaver, chunkSize int) error
	// GetTable returns the table from given table name.
	GetTable(projectId, datasetId, tableName string) *bigquery.Table
	// CreateTemporaryTable creates a temporary table with the same schema as the given table.
	// Returns the temporary table id.
	CreateTemporaryTable(projectId, datasetId, tableName string) (*_bq.Identifier, error)
	// ExecuteQuery executes a SQL query and returns the result.
	ExecuteQuery(ctx context.Context, sql string) error
	// GetTableColumns returns column names for a table.
	GetTableColumns(ctx context.Context, projectId, datasetId, tableName string) ([]string, error)
}

type client struct {
	bigqueryClient *bigquery.Client
}

func NewBigQueryClient(ctx context.Context, cfg config.BigQuery) (Client, error) {
	if cfg.CredentialsFile == "" && os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") == "" {
		return nil, errors.New("credentials file or GOOGLE_APPLICATION_CREDENTIALS environment variable is required")
	}
	bigqueryClient, err := bigquery.NewClient(ctx, bigquery.DetectProjectID, option.WithCredentialsFile(cfg.CredentialsFile))
	if err != nil {
		return nil, err
	}
	return &client{bigqueryClient: bigqueryClient}, nil
}

func (b *client) BatchInsert(ctx context.Context, table *bigquery.Table, data []bigquery.ValueSaver, chunkSize int) error {
	for chunk := range slices.Chunk(data, chunkSize) {
		err := b.Insert(ctx, table, chunk)
		if err != nil {
			return err
		}
	}

	return nil
}

func (b *client) Insert(ctx context.Context, table *bigquery.Table, data any) error {
	inserter := table.Inserter()

	if err := inserter.Put(ctx, data); err != nil {
		return fmt.Errorf("writing to table failed table id: %s, err: %v", table.TableID, err)
	}

	return nil
}

func (c *client) GetTable(projectId, datasetId, tableName string) *bigquery.Table {
	return c.bigqueryClient.DatasetInProject(projectId, datasetId).Table(tableName)
}

func (c *client) CreateTemporaryTable(projectId, datasetId, tableName string) (*_bq.Identifier, error) {
	temporaryTableId := c.createTemporaryTableId(tableName)
	table := c.GetTable(projectId, datasetId, tableName)
	metadata, err := table.Metadata(context.Background())
	if err != nil {
		targetTable := fmt.Sprintf("%s.%s.%s", projectId, datasetId, tableName)
		return nil, fmt.Errorf("target table '%s' does not exist: %w", targetTable, err)
	}

	// Add additional fields to the schema for tracking events
	schema := append(metadata.Schema,
		&bigquery.FieldSchema{
			Name:     config.EventTime,
			Type:     bigquery.TimestampFieldType,
			Required: true,
		},
		&bigquery.FieldSchema{
			Name:     config.OperationType,
			Type:     bigquery.StringFieldType,
			Required: true,
		},
	)

	expirationTime := time.Now().UTC().Add(time.Hour * 48)
	metadataToCreate := &bigquery.TableMetadata{
		Schema:         schema,
		Name:           temporaryTableId,
		ExpirationTime: expirationTime,
	}

	tableRef := c.bigqueryClient.DatasetInProject(projectId, datasetId).Table(temporaryTableId)
	if err = tableRef.Create(context.Background(), metadataToCreate); err != nil {
		return nil, err
	}

	return _bq.NewIdentifier(projectId, datasetId, temporaryTableId), nil
}

func (c *client) ExecuteQuery(ctx context.Context, sql string) error {
	query := c.bigqueryClient.Query(sql)
	job, err := query.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to run query: %w", err)
	}

	status, err := job.Wait(ctx)
	if err != nil {
		return fmt.Errorf("failed to wait for query completion: %w", err)
	}

	if status.Err() != nil {
		return fmt.Errorf("query execution failed: %w", status.Err())
	}

	return nil
}

func (c *client) GetTableColumns(ctx context.Context, projectId, datasetId, tableName string) ([]string, error) {
	table := c.GetTable(projectId, datasetId, tableName)
	metadata, err := table.Metadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	columns := make([]string, 0, len(metadata.Schema))
	for _, field := range metadata.Schema {
		columns = append(columns, field.Name)
	}

	return columns, nil
}

func (c *client) createTemporaryTableId(tableName string) string {
	return fmt.Sprintf("%s_%s", tableName, uuid.New().String())
}
