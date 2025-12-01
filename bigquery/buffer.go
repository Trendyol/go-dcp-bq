package bigquery

import (
	"sync"
	"time"
)

type BatchBuffer struct {
	Items       []Row
	Ticker      *time.Ticker
	MaxSize     int
	Mu          sync.Mutex
	SourceTable Identifier
}
