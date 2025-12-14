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
