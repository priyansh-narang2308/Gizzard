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

		// Determine the local IP dynamically or use an explicit one
		localAddr := "localhost"
		if outboundIP := getOutboundIP(); outboundIP != nil {
			localAddr = outboundIP.String()
		}

		msg := protocol.Message{
			Type:   "REGISTER",
			Sender: n.ID,
			Payload: map[string]string{
				"address": localAddr + ":" + n.Port,
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

// Helper to reliably find the local machine's IP address on the network
func getOutboundIP() net.IP {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return nil
	}
	defer conn.Close()
	localAddr := conn.LocalAddr().(*net.UDPAddr)
	return localAddr.IP
}
