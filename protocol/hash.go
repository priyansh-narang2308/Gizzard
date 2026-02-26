package protocol

import (
	"hash/fnv"
)

const TotalShards = 8

// GetShardID computes which shard a key belongs to using FNV-1a hashing.
// This is deterministic: the same key always results in the same shard ID.
func GetShardID(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() % uint32(TotalShards))
}
