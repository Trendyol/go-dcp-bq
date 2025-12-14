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
