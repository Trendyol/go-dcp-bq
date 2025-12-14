package bigquery

import (
	"fmt"
	"strings"

	"github.com/Trendyol/go-dcp-bq/config"
)

// BuildQuery builds a query that handles both upsert and delete operations from a single table
// It ensures that for each __cb_key, only the latest event (by __event_time) is processed
func BuildQuery(project, dataset, table, sourceTable string, columns []string) string {
	setClauses := make([]string, 0, len(columns))
	values := make([]string, 0, len(columns))

	for _, col := range columns {
		if col == config.CBKey || col == config.OperationType || col == config.EventTime {
			continue
		}
		setClauses = append(setClauses, fmt.Sprintf("%s = S.%s", col, col))
		values = append(values, "S."+col)
	}

	// Build a multi-statement script using BEGIN...END
	query := fmt.Sprintf(
		"BEGIN\n"+
			"  -- Delete records\n"+
			"  DELETE FROM `%s.%s.%s`\n"+
			"  WHERE __cb_key IN (\n"+
			"    WITH latest_events AS (\n"+
			"      SELECT *,\n"+
			"        ROW_NUMBER() OVER (PARTITION BY __cb_key ORDER BY __event_time DESC) as rn\n"+
			"      FROM `%s`\n"+
			"    )\n"+
			"    SELECT __cb_key FROM latest_events \n"+
			"    WHERE rn = 1 AND __operation_type = 'DELETE'\n"+
			"  );\n\n"+
			"  -- Merge (upsert) records\n"+
			"  MERGE INTO `%s.%s.%s` AS T\n"+
			"  USING (\n"+
			"    WITH latest_events AS (\n"+
			"      SELECT *,\n"+
			"        ROW_NUMBER() OVER (PARTITION BY __cb_key ORDER BY __event_time DESC) as rn\n"+
			"      FROM `%s`\n"+
			"    )\n"+
			"    SELECT * FROM latest_events \n"+
			"    WHERE rn = 1 AND __operation_type = 'UPSERT'\n"+
			"  ) AS S\n"+
			"  ON T.__cb_key = S.__cb_key\n"+
			"  WHEN MATCHED THEN\n"+
			"    UPDATE SET %s\n"+
			"  WHEN NOT MATCHED THEN\n"+
			"    INSERT (__cb_key, %s)\n"+
			"    VALUES (S.__cb_key, %s);\n"+
			"END",
		project, dataset, table,
		sourceTable,
		project, dataset, table,
		sourceTable,
		strings.Join(setClauses, ", "),
		strings.Join(columns, ", "),
		strings.Join(values, ", "),
	)

	return query
}
