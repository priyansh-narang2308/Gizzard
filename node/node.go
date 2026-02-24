package node

import (
	"encoding/json"
	"fmt"
	"net"
	"time"

	"github.com/priyansh-narang2308/gizzard/protocol"
)

type Node struct {
	ID     string
	Master string
	Port   string
}

func (n *Node) Start() {
	n.register()

	go n.sendHeartbeats()

	select {}
}

func (n *Node) register() {
	conn, _ := net.Dial("tcp", n.Master)
	defer conn.Close()

	msg := protocol.Message{
		Type:   "REGISTER",
		Sender: n.ID,
		Payload: map[string]string{
			"address": "localhost:" + n.Port,
		},
	}

	json.NewEncoder(conn).Encode(msg)
	fmt.Println("Registered with master")
}

func (n *Node) sendHeartbeats() {
	for {
		time.Sleep(5 * time.Second)

		conn, err := net.Dial("tcp", n.Master)
		if err != nil {
			continue
		}

		msg := protocol.Message{
			Type:   "HEARTBEAT",
			Sender: n.ID,
		}

		json.NewEncoder(conn).Encode(msg)
		conn.Close()
	}
}