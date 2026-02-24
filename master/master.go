package master

import (
	"encoding/json"
	"fmt"
	"net"
	"sort"
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
	listener, err := net.Listen("tcp", ":"+port)
	if err != nil {
		panic(err)
	}
	fmt.Println("Master listening on port", port)

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

	fmt.Println("Node registered:", nodeID)

	m.assignShards()
}

func (m *Master) updateHeartbeat(msg protocol.Message) {
	m.Mu.Lock()
	defer m.Mu.Unlock()

	if node, ok := m.Nodes[msg.Sender]; ok {
		node.LastSeen = time.Now()
		node.Status = "ALIVE"
	}
}

func (m *Master) assignShards() {
	totalShards := 8
	var aliveNodeIDs []string

	// Only collect ALIVE nodes
	for id, node := range m.Nodes {
		if node.Status == "ALIVE" {
			aliveNodeIDs = append(aliveNodeIDs, id)
		}
	}

	// Sort node IDs to make assignments deterministic
	sort.Strings(aliveNodeIDs)

	// If no alive nodes exist, clear all leaders but keep shards existing
	if len(aliveNodeIDs) == 0 {
		for i := 0; i < totalShards; i++ {
			if _, exists := m.Shards[i]; !exists {
				m.Shards[i] = &model.Shard{ID: i}
			}
			m.Shards[i].Leader = "NONE"
		}
		return
	}

	// Deterministically assign shards to ALIVE nodes in round-robin fashion
	for i := 0; i < totalShards; i++ {
		leader := aliveNodeIDs[i%len(aliveNodeIDs)]
		m.Shards[i] = &model.Shard{
			ID:     i,
			Leader: leader,
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
				fmt.Println("Node marked DEAD:", node.ID)
				failureDetected = true
			}
		}

		if failureDetected {
			fmt.Println("Cluster state changed. Rebalancing shards...")
			m.assignShards()
		}
		m.Mu.Unlock()
	}
}
