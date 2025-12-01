package bigquery

import "fmt"

type Identifier struct {
	ProjectId string
	DatasetId string
	TableName string
}

func NewIdentifier(projectId, datasetId, tableName string) *Identifier {
	return &Identifier{ProjectId: projectId, DatasetId: datasetId, TableName: tableName}
}

func (i *Identifier) ToTableId() string {
	return fmt.Sprintf("%s.%s.%s", i.ProjectId, i.DatasetId, i.TableName)
}
