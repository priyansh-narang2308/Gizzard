package node

import (
	"encoding/json"
	"log"
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
	for {
		conn, err := net.Dial("tcp", n.Master)
		if err != nil {
			log.Printf("[NODE %s] Failed to connect to master at %s: %v. Retrying in 5s...\n", n.ID, n.Master, err)
			time.Sleep(5 * time.Second)
			continue
		}

		msg := protocol.Message{
			Type:   "REGISTER",
			Sender: n.ID,
			Payload: map[string]string{
				"address": "localhost:" + n.Port,
			},
		}

		err = json.NewEncoder(conn).Encode(msg)
		conn.Close()

		if err != nil {
			log.Printf("[NODE %s] Failed to send register message: %v. Retrying in 5s...\n", n.ID, err)
			time.Sleep(5 * time.Second)
			continue
		}

		log.Printf("[NODE %s] Successfully registered with master at %s\n", n.ID, n.Master)
		break
	}
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
