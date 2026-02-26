package model

import "time"

type NodeInfo struct {
	ID       string
	Address  string
	LastSeen time.Time
	Status   string
}

type Shard struct {
	ID     int
	Leader string
	Keys   int
}
