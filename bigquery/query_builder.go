package bigquery

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/Trendyol/go-dcp-bq/config"
)

var validIdentifier = regexp.MustCompile(`^[a-zA-Z0-9_][a-zA-Z0-9_-]*$`)

func sanitizeIdentifier(name string, includeBackticks bool) string {
	if !validIdentifier.MatchString(name) {
		panic(fmt.Sprintf("invalid BigQuery identifier: %q", name))
	}
	if includeBackticks {
		return "`" + name + "`"
	}
	return name
}

func sanitizeTableName(fullName string) string {
	parts := strings.Split(fullName, ".")
	if len(parts) != 3 {
		panic("invalid BigQuery table name")
	}

	sanitizedParts := make([]string, len(parts))
	for i, p := range parts {
		sanitizedParts[i] = sanitizeIdentifier(p, false)
	}

	return strings.Join(sanitizedParts, ".")
}

// BuildQuery builds a query that handles both upsert and delete operations from a single table
// It ensures that for each __cb_key, only the latest event (by __event_time) is processed
func BuildQuery(project, dataset, table, sourceTable string, columns []string) string {
	setClauses := make([]string, 0, len(columns))
	values := make([]string, 0, len(columns))
	sanitizedColumns := make([]string, 0, len(columns))

	for _, col := range columns {
		if col == config.CBKey || col == config.OperationType || col == config.EventTime {
			continue
		}
		safe := sanitizeIdentifier(col, true)
		setClauses = append(setClauses, fmt.Sprintf("%s = S.%s", safe, safe))
		values = append(values, "S."+safe)
		sanitizedColumns = append(sanitizedColumns, safe)
	}

	safeProject := sanitizeIdentifier(project, true)
	safeDataset := sanitizeIdentifier(dataset, true)
	safeTable := sanitizeIdentifier(table, true)
	safeSourceTable := sanitizeTableName(sourceTable)

	targetRef := fmt.Sprintf("%s.%s.%s", safeProject, safeDataset, safeTable)

	// Build a multi-statement script using BEGIN...END
	query := fmt.Sprintf(
		"BEGIN\n"+
			"  -- Delete records\n"+
			"  DELETE FROM %s\n"+
			"  WHERE __cb_key IN (\n"+
			"    WITH latest_events AS (\n"+
			"      SELECT *,\n"+
			"        ROW_NUMBER() OVER (PARTITION BY __cb_key ORDER BY __event_time DESC) as rn\n"+
			"      FROM %s\n"+
			"    )\n"+
			"    SELECT __cb_key FROM latest_events \n"+
			"    WHERE rn = 1 AND __operation_type = 'DELETE'\n"+
			"  );\n\n"+
			"  -- Merge (upsert) records\n"+
			"  MERGE INTO %s AS T\n"+
			"  USING (\n"+
			"    WITH latest_events AS (\n"+
			"      SELECT *,\n"+
			"        ROW_NUMBER() OVER (PARTITION BY __cb_key ORDER BY __event_time DESC) as rn\n"+
			"      FROM %s\n"+
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
		targetRef,
		safeSourceTable,
		targetRef,
		safeSourceTable,
		strings.Join(setClauses, ", "),
		strings.Join(sanitizedColumns, ", "),
		strings.Join(values, ", "),
	)

	return query
}
