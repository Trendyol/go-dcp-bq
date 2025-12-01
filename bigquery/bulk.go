package bigquery

import (
	"time"

	"github.com/Trendyol/go-dcp/models"
)

type Bulk interface {
	StartBulk()
	AddActions(ctx *models.ListenerContext, eventTime time.Time, actions []Row) error
	GetMetric() *Metric
	PrepareStartRebalancing()
	PrepareEndRebalancing()
	Close() error
}
