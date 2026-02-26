package master

import (
	"encoding/json"
	"log"
	"net"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/priyansh-narang2308/gizzard/model"
	"github.com/priyansh-narang2308/gizzard/protocol"
)

type Master struct {
	Nodes  map[string]*model.NodeInfo
	Shards map[int]*model.Shard
	Mu     sync.Mutex
}

func NewMaster() *Master {
	return &Master{
		Nodes:  make(map[string]*model.NodeInfo),
		Shards: make(map[int]*model.Shard),
	}
}

func (m *Master) StartTCP(port string) {
	listener, err := net.Listen("tcp", "0.0.0.0:"+port)
	if err != nil {
		log.Fatalf("[MASTER] Failed to start TCP listener on port %s: %v", port, err)
	}
	log.Printf("[MASTER] Listening on TCP port %s\n", port)

	go m.monitorFailures()

	for {
		conn, _ := listener.Accept()
		go m.handleConnection(conn)
	}
}

func (m *Master) handleConnection(conn net.Conn) {
	defer conn.Close()

	var msg protocol.Message
	json.NewDecoder(conn).Decode(&msg)

	switch msg.Type {

	case "REGISTER":
		m.registerNode(msg)

	case "HEARTBEAT":
		m.updateHeartbeat(msg)

		m.Mu.Lock()
		shards := make(map[string]string)
		for _, s := range m.Shards {
			if s.Leader != "NONE" && s.Leader != "" {
				if nodeInfo, ok := m.Nodes[s.Leader]; ok {
					shards[strconv.Itoa(s.ID)] = nodeInfo.Address
				}
			}
		}
		m.Mu.Unlock()

		json.NewEncoder(conn).Encode(protocol.Message{
			Type:    "ROUTING",
			Payload: shards,
		})
	}
}

func (m *Master) registerNode(msg protocol.Message) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	nodeID := msg.Sender
	address := msg.Payload["address"]

	m.Nodes[nodeID] = &model.NodeInfo{
		ID:       nodeID,
		Address:  address,
		LastSeen: time.Now(),
		Status:   "ALIVE",
	}

	log.Printf("[EVENT] Node %s registered successfully (Address: %s)\n", nodeID, address)

	m.assignShards()
}

func (m *Master) updateHeartbeat(msg protocol.Message) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	if node, ok := m.Nodes[msg.Sender]; ok {
		node.LastSeen = time.Now()
		node.Status = "ALIVE"

		// Update shard key counts reported by the node
		for shardIDStr, keyCountStr := range msg.Payload {
			shardID, _ := strconv.Atoi(shardIDStr)
			keyCount, _ := strconv.Atoi(keyCountStr)
			if shard, exists := m.Shards[shardID]; exists && shard.Leader == msg.Sender {
				shard.Keys = keyCount
			}
		}
	}
}

func (m *Master) assignShards() {
	var aliveNodeIDs []string

	// Only collect ALIVE nodes
	for id, node := range m.Nodes {
		if node.Status == "ALIVE" {
			aliveNodeIDs = append(aliveNodeIDs, id)
		}
	}

	// Sort node IDs to make assignments deterministic
	sort.Slice(aliveNodeIDs, func(i, j int) bool {
		id1, err1 := strconv.Atoi(aliveNodeIDs[i])
		id2, err2 := strconv.Atoi(aliveNodeIDs[j])
		if err1 == nil && err2 == nil {
			return id1 < id2
		}
		return aliveNodeIDs[i] < aliveNodeIDs[j]
	})

	// If no alive nodes exist, clear all leaders but keep shards existing
	if len(aliveNodeIDs) == 0 {
		for i := 0; i < protocol.TotalShards; i++ {
			if shard, exists := m.Shards[i]; exists {
				shard.Leader = "NONE"
			} else {
				m.Shards[i] = &model.Shard{ID: i, Leader: "NONE"}
			}
		}
		return
	}

	// Deterministically assign shards to ALIVE nodes in round-robin fashion
	for i := range protocol.TotalShards {
		leader := aliveNodeIDs[i%len(aliveNodeIDs)]
		if shard, exists := m.Shards[i]; exists {
			shard.Leader = leader
		} else {
			m.Shards[i] = &model.Shard{
				ID:     i,
				Leader: leader,
			}
		}
	}
}

func (m *Master) monitorFailures() {
	for {
		time.Sleep(5 * time.Second)

		failureDetected := false

		m.Mu.Lock()
		for _, node := range m.Nodes {
			if node.Status == "ALIVE" && time.Since(node.LastSeen) > 10*time.Second {
				node.Status = "DEAD"
				log.Printf("[ALERT] Node %s missed heartbeats. Marked as DEAD.\n", node.ID)
				failureDetected = true
			}
		}

		if failureDetected {
			log.Println("[INFO] Cluster state changed. Rebalancing shards deterministically...")
			m.assignShards()
		}
		m.Mu.Unlock()
	}
}
