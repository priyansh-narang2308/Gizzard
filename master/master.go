package master	

import (
	"encoding/json"
	"fmt"
	"net"
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
	nodeIDs := []string{}

	for id := range m.Nodes {
		nodeIDs = append(nodeIDs, id)
	}

	if len(nodeIDs) == 0 {
		return
	}

	for i := 0; i < totalShards; i++ {
		leader := nodeIDs[i%len(nodeIDs)]
		m.Shards[i] = &model.Shard{
			ID:     i,
			Leader: leader,
		}
	}
}

func (m *Master) monitorFailures() {
	for {
		time.Sleep(5 * time.Second)

		m.Mu.Lock()
		for _, node := range m.Nodes {
			if time.Since(node.LastSeen) > 10*time.Second {
				node.Status = "DEAD"
				fmt.Println("Node marked DEAD:", node.ID)
			}
		}
		m.Mu.Unlock()
	}
}
