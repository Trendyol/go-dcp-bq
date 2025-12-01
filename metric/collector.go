package metric

import (
	_bq "github.com/Trendyol/go-dcp-bq/bigquery"
	"github.com/Trendyol/go-dcp/helpers"
	"github.com/prometheus/client_golang/prometheus"
)

type Collector struct {
	bulk _bq.Bulk

	processLatency            *prometheus.Desc
	bulkRequestProcessLatency *prometheus.Desc
}

func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	prometheus.DescribeByCollect(c, ch)
}

func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	bulkMetric := c.bulk.GetMetric()

	ch <- prometheus.MustNewConstMetric(
		c.processLatency,
		prometheus.GaugeValue,
		float64(bulkMetric.ProcessLatencyMs),
		[]string{}...,
	)

	ch <- prometheus.MustNewConstMetric(
		c.bulkRequestProcessLatency,
		prometheus.GaugeValue,
		float64(bulkMetric.BulkRequestProcessLatencyMs),
		[]string{}...,
	)
}

func NewMetricCollector(bulk _bq.Bulk) *Collector {
	return &Collector{
		bulk: bulk,

		processLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "bigquery_connector_latency_ms", "current"),
			"BigQuery connector latency ms",
			[]string{},
			nil,
		),

		bulkRequestProcessLatency: prometheus.NewDesc(
			prometheus.BuildFQName(helpers.Name, "bigquery_connector_bulk_request_process_latency_ms", "current"),
			"BigQuery connector bulk request process latency ms",
			[]string{},
			nil,
		),
	}
}
