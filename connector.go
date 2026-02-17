package dcpbq

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	jsoniter "github.com/json-iterator/go"

	dcpCouchbase "github.com/Trendyol/go-dcp/couchbase"

	_bq "github.com/Trendyol/go-dcp-bq/bigquery"
	"github.com/Trendyol/go-dcp-bq/bigquery/v1/bulk"
	"github.com/Trendyol/go-dcp-bq/mapper"
	"github.com/Trendyol/go-dcp-bq/metric"

	"github.com/Trendyol/go-dcp"
	"github.com/Trendyol/go-dcp-bq/config"
	"github.com/Trendyol/go-dcp-bq/couchbase"
	"github.com/Trendyol/go-dcp/logger"
	"github.com/Trendyol/go-dcp/models"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v3"
)

type Connector interface {
	Start()
	Close()
	GetDcpClient() dcpCouchbase.Client
}

type connector struct {
	dcp    dcp.Dcp
	mapper mapper.Service
	config *config.Connector
	bulk   _bq.Bulk
}

func (c *connector) Start() {
	go func() {
		<-c.dcp.WaitUntilReady()
		c.bulk.StartBulk()
	}()
	c.dcp.Start()
}

func (c *connector) Close() {
	c.dcp.Close()
	c.bulk.Close()
}

func (c *connector) GetDcpClient() dcpCouchbase.Client {
	return c.dcp.GetClient()
}

func (c *connector) listener(ctx *models.ListenerContext) {
	// Initialize ListenerTrace for current listen operation
	listenerTrace := ctx.ListenerTracerComponent.InitializeListenerTrace("Listen", nil)
	defer listenerTrace.Finish()
	var e couchbase.Event
	switch event := ctx.Event.(type) {
	case models.DcpMutation:
		e = couchbase.NewMutateEvent(listenerTrace, event.Key, event.Value, event.CollectionName, event.EventTime, event.Cas, event.VbID)
		c.handleUpsert(ctx, e)
	case models.DcpExpiration:
		e = couchbase.NewExpireEvent(listenerTrace, event.Key, nil, event.CollectionName, event.EventTime, event.Cas, event.VbID)
		c.handleDelete(ctx, e)
	case models.DcpDeletion:
		e = couchbase.NewDeleteEvent(listenerTrace, event.Key, nil, event.CollectionName, event.EventTime, event.Cas, event.VbID)
		c.handleDelete(ctx, e)
	default:
		return
	}
}

func (c *connector) handleUpsert(ctx *models.ListenerContext, e couchbase.Event) {
	actions, err := c.mapper.TransformEvents([]couchbase.Event{e})
	if err != nil {
		logger.Log.Error("Cannot map event, error: %v", err)
		return
	}
	for i := range actions {
		actions[i].Ctx = ctx
		actions[i].OperationType = _bq.OperationUpsert
		actions[i].EventTime = e.EventTime
	}

	if len(actions) == 0 {
		ctx.Ack()
		return
	}

	c.bulk.AddActions(ctx, e.EventTime, actions)
}

func (c *connector) handleDelete(ctx *models.ListenerContext, e couchbase.Event) {
	actions := []_bq.Row{*_bq.NewRow(map[string]any{config.CBKey: string(e.Key)}, _bq.OperationDelete)}
	for i := range actions {
		actions[i].Ctx = ctx
		actions[i].OperationType = _bq.OperationDelete
		actions[i].EventTime = e.EventTime
	}
	c.bulk.AddActions(ctx, e.EventTime, actions)
}

type ConnectorBuilder struct {
	mapper       mapper.Service
	bqAPIVersion _bq.APIVersion
	config       any
}

func newConnectorConfigFromPath(path string) (*config.Connector, error) {
	cleanPath := filepath.Clean(path)
	if strings.Contains(cleanPath, "..") {
		return nil, fmt.Errorf("invalid config path: path traversal is not allowed")
	}

	file, err := os.ReadFile(cleanPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read config file: %w", err)
	}
	var c config.Connector
	err = yaml.Unmarshal(file, &c)
	if err != nil {
		return nil, fmt.Errorf("failed to parse config file: %w", err)
	}
	return &c, nil
}

func newConfig(cf any) (*config.Connector, error) {
	switch v := cf.(type) {
	case *config.Connector:
		return v, nil
	case config.Connector:
		return &v, nil
	case string:
		return newConnectorConfigFromPath(v)
	default:
		return nil, errors.New("invalid config")
	}
}

func newConnector(cf any, mapper mapper.Service, bqAPIVersion _bq.APIVersion) (Connector, error) {
	cfg, err := newConfig(cf)
	if err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()

	connector := &connector{
		mapper: mapper,
		config: cfg,
	}

	dcp, err := dcp.NewDcp(&cfg.Dcp, connector.listener)
	if err != nil {
		logger.Log.Error("Dcp error: %v", err)
		return nil, err
	}

	copyOfConfig := cfg.BigQuery
	printConfiguration(copyOfConfig)

	dcpConfig := dcp.GetConfig()
	dcpConfig.Checkpoint.Type = "manual"

	connector.dcp = dcp

	if bqAPIVersion == _bq.V1 {
		bulk, err := bulk.New(cfg, dcp.Commit)
		if err != nil {
			panic(err)
		}
		connector.bulk = bulk
	} else {
		panic("unsupported bigquery api version")
	}

	connector.dcp.SetEventHandler(
		&DcpEventHandler{
			isFinite: dcpConfig.IsDcpModeFinite(),
			bulk:     connector.bulk,
		})

	metricCollector := metric.NewMetricCollector(connector.bulk)
	dcp.SetMetricCollectors(metricCollector)

	return connector, nil
}

func NewConnectorBuilder(config any) ConnectorBuilder {
	return ConnectorBuilder{
		config: config,
		mapper: mapper.NewDefaultMapper(),
	}
}

// SetMapper configures the data transformation strategy for converting Couchbase DCP events to BigQuery rows.
// Available mapper implementations:
//
// 1. Default Mapper (mapper.NewDefaultMapper()):
//   - Provides simple 1:1 field mapping without any transformations
//
// 2. Custom Mapper (mapper.New(config)):
//   - Provides configurable field mappings using mapping rules
//   - Supports nested field access with dot notation (e.g., "user.profile.name" -> "username")
//
// Example usage:
//
//	// Using default mapper (no configuration needed)
//	builder.SetMapper(mapper.NewDefaultMapper())
//
//	// Using custom mapper with field mappings
//	config := &mapper.Config{
//	    Mappings: map[string]string{
//	        "user.id":    "user_id",
//	        "user.email": "email_address",
//	        "timestamp":  "created_at",
//	    },
//	}
//	builder.SetMapper(mapper.New(config))
func (c ConnectorBuilder) SetMapper(mapper mapper.Service) ConnectorBuilder {
	c.mapper = mapper
	return c
}

// SetBigqueryAPIVersion configures which BigQuery API version to use for data operations.
// Currently supported API versions:
//
// - bigquery.V1 (default)
//
// Note: Only V1 is currently implemented. Future versions may be added to support
// new BigQuery features or optimizations as they become available.
//
// Example usage:
//
//	builder.SetBigqueryAPIVersion(bigquery.V1)
func (c ConnectorBuilder) SetBigqueryAPIVersion(version _bq.APIVersion) ConnectorBuilder {
	c.bqAPIVersion = version
	return c
}

func (c ConnectorBuilder) Build() (Connector, error) {
	return newConnector(c.config, c.mapper, c.bqAPIVersion)
}

func (c ConnectorBuilder) SetLogger(logrus *logrus.Logger) ConnectorBuilder {
	logger.Log = &logger.Loggers{
		Logrus: logrus,
	}
	return c
}

func printConfiguration(config config.BigQuery) {
	configJSON, _ := jsoniter.Marshal(config)

	dst := &bytes.Buffer{}
	if err := json.Compact(dst, configJSON); err != nil {
		logger.Log.Error("error while print bigquery configuration, err: %v", err)
		panic(err)
	}

	logger.Log.Info("using bigquery config: %v", dst.String())
}
