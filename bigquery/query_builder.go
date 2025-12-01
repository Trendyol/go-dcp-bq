package bigquery

import (
	"fmt"
	"strings"

	"github.com/Trendyol/go-dcp-bq/config"
)

// BuildQuery builds a query that handles both upsert and delete operations from a single table
// It ensures that for each __cb_key, only the latest event (by __event_time) is processed
func BuildQuery(project, dataset, table, sourceTable string, columns []string) string {
	latestEventsCTE := fmt.Sprintf(
		"WITH latest_events AS (\n"+
			"  SELECT *,\n"+
			"    ROW_NUMBER() OVER (PARTITION BY __cb_key ORDER BY __event_time DESC) as rn\n"+
			"  FROM `%s`\n"+
			"),\n"+
			"latest_events_filtered AS (\n"+
			"  SELECT * FROM latest_events WHERE rn = 1\n"+
			")",
		sourceTable,
	)

	deleteQuery := fmt.Sprintf(
		"\nDELETE FROM `%s.%s.%s`\n"+
			"WHERE __cb_key IN (\n"+
			"  SELECT __cb_key FROM latest_events_filtered WHERE __operation_type = 'DELETE'\n"+
			");\n\n",
		project, dataset, table,
	)

	setClauses := make([]string, 0, len(columns))
	values := make([]string, 0, len(columns))

	for _, col := range columns {
		if col == config.CBKey || col == config.OperationType || col == config.EventTime {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = S.%s", col, col))
		values = append(values, "S."+col)
	}

	upsertQuery := fmt.Sprintf(
		"MERGE INTO `%s.%s.%s` AS T\n"+
			"USING (SELECT * FROM latest_events_filtered WHERE __operation_type = 'UPSERT') AS S\n"+
			"ON T.__cb_key = S.__cb_key\n"+
			"WHEN MATCHED THEN\n"+
			"  UPDATE SET %s\n"+
			"WHEN NOT MATCHED THEN\n"+
			"  INSERT (__cb_key, %s)\n"+
			"  VALUES (S.__cb_key, %s)",
		project, dataset, table,
		strings.Join(setClauses, ", "),
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)

	return latestEventsCTE + deleteQuery + upsertQuery
}
