package node

import (
	"encoding/json"
	"hash/fnv"
	"log"
	"net"
	"strconv"
	"sync"
	"time"

	"github.com/priyansh-narang2308/gizzard/protocol"
)

type Node struct {
	ID      string
	Master  string
	Port    string
	Data    map[int]map[string]string
	Mu      sync.RWMutex
	Routing map[string]string
}

func (n *Node) Start() {
	n.Data = make(map[int]map[string]string)
	n.Routing = make(map[string]string)

	n.register()

	go n.sendHeartbeats()

	// Start listening for data requests (PUT/GET)
	listener, err := net.Listen("tcp", "0.0.0.0:"+n.Port)
	if err != nil {
		log.Fatalf("[NODE %s] Failed to start listener on port %s: %v", n.ID, n.Port, err)
	}
	log.Printf("[NODE %s] Listening for data requests on port %s\n", n.ID, n.Port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Printf("[NODE %s] Accept error: %v\n", n.ID, err)
			continue
		}
		go n.handleRequest(conn)
	}
}

func (n *Node) handleRequest(conn net.Conn) {
	defer conn.Close()

	var msg protocol.Message
	if err := json.NewDecoder(conn).Decode(&msg); err != nil {
		return
	}

	response := map[string]string{"status": "ok"}
	switch msg.Type {
	case "PUT":
		n.handlePut(msg, conn)
		return
	case "GET":
		n.handleGet(msg, conn)
		return
	default:
		response["status"] = "error"
		response["message"] = "Unknown command"
	}

	json.NewEncoder(conn).Encode(response)
}

func (n *Node) handlePut(msg protocol.Message, conn net.Conn) {
	key := msg.Payload["key"]
	val := msg.Payload["value"]

	totalShards := 8

	h := fnv.New32a()
	h.Write([]byte(key))
	shardID := int(h.Sum32() % uint32(totalShards))
	shardStr := strconv.Itoa(shardID)

	n.Mu.RLock()
	ownerAddr := n.Routing[shardStr]
	localAddr := "localhost"
	if outboundIP := getOutboundIP(); outboundIP != nil {
		localAddr = outboundIP.String()
	}
	ownAddr := localAddr + ":" + n.Port
	n.Mu.RUnlock()

	if ownerAddr == "" {
		json.NewEncoder(conn).Encode(map[string]string{
			"status":  "error",
			"message": "Shard owner not found",
		})
		return
	}

	if ownerAddr != ownAddr {
		log.Printf("[NODE %s] Forwarding PUT for key %s (Shard %d) to %s\n", n.ID, key, shardID, ownerAddr)

		fwdConn, err := net.Dial("tcp", ownerAddr)
		if err != nil {
			json.NewEncoder(conn).Encode(map[string]string{
				"status":  "error",
				"message": "Failed to forward request",
			})
			return
		}
		defer fwdConn.Close()

		json.NewEncoder(fwdConn).Encode(msg)

		var fwdResp map[string]interface{}
		json.NewDecoder(fwdConn).Decode(&fwdResp)
		json.NewEncoder(conn).Encode(fwdResp)
		return
	}

	n.Mu.Lock()
	if _, exists := n.Data[shardID]; !exists {
		n.Data[shardID] = make(map[string]string)
	}
	n.Data[shardID][key] = val
	n.Mu.Unlock()

	log.Printf("[DATA] Stored Key: %s, Val: %s in Shard: %d\n", key, val, shardID)

	json.NewEncoder(conn).Encode(map[string]string{
		"status":  "ok",
		"message": "Data stored successfully",
	})
}

func (n *Node) handleGet(msg protocol.Message, conn net.Conn) {
	key := msg.Payload["key"]

	totalShards := 8

	h := fnv.New32a()
	h.Write([]byte(key))
	shardID := int(h.Sum32() % uint32(totalShards))
	shardStr := strconv.Itoa(shardID)

	n.Mu.RLock()
	ownerAddr := n.Routing[shardStr]
	localAddr := "localhost"
	if outboundIP := getOutboundIP(); outboundIP != nil {
		localAddr = outboundIP.String()
	}
	ownAddr := localAddr + ":" + n.Port
	n.Mu.RUnlock()

	if ownerAddr == "" {
		json.NewEncoder(conn).Encode(map[string]string{
			"status":  "error",
			"message": "Shard owner not found",
		})
		return
	}

	if ownerAddr != ownAddr {
		log.Printf("[NODE %s] Forwarding GET for key %s (Shard %d) to %s\n", n.ID, key, shardID, ownerAddr)

		fwdConn, err := net.Dial("tcp", ownerAddr)
		if err != nil {
			json.NewEncoder(conn).Encode(map[string]string{
				"status":  "error",
				"message": "Failed to forward request",
			})
			return
		}
		defer fwdConn.Close()

		json.NewEncoder(fwdConn).Encode(msg)

		var fwdResp map[string]interface{}
		json.NewDecoder(fwdConn).Decode(&fwdResp)
		json.NewEncoder(conn).Encode(fwdResp)
		return
	}

	n.Mu.RLock()
	var val string
	var exists bool
	if shardData, ok := n.Data[shardID]; ok {
		val, exists = shardData[key]
	}
	n.Mu.RUnlock()

	if !exists {
		json.NewEncoder(conn).Encode(map[string]string{
			"status":  "error",
			"message": "Key not found",
		})
		return
	}

	log.Printf("[DATA] Retrieved Key: %s, Val: %s from Shard: %d\n", key, val, shardID)

	json.NewEncoder(conn).Encode(map[string]string{
		"status": "ok",
		"value":  val,
	})
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

		n.Mu.RLock()
		stats := make(map[string]string)
		for shardID, items := range n.Data {
			stats[strconv.Itoa(shardID)] = strconv.Itoa(len(items))
		}
		n.Mu.RUnlock()

		msg := protocol.Message{
			Type:    "HEARTBEAT",
			Sender:  n.ID,
			Payload: stats,
		}

		json.NewEncoder(conn).Encode(msg)

		var resp protocol.Message
		if err := json.NewDecoder(conn).Decode(&resp); err == nil && resp.Type == "ROUTING" {
			n.Mu.Lock()
			n.Routing = resp.Payload
			n.Mu.Unlock()
		}

		conn.Close()
	}
}

// Helper to reliably find the local machine's IP address on the network
func getOutboundIP() net.IP {
	// Try establishing a dummy UDP connection to Google DNS (doesn't actually send packets)
	// This is the cleanest way to get the preferred outbound IP.
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err == nil {
		defer conn.Close()
		localAddr := conn.LocalAddr().(*net.UDPAddr)
		return localAddr.IP
	}

	// Fallback for Windows/Strict Firewalls: iterate over network interfaces
	addrs, err := net.InterfaceAddrs()
	if err == nil {
		for _, address := range addrs {
			// Check the address type and if it is not a loopback then return it
			if ipnet, ok := address.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
				if ipnet.IP.To4() != nil {
					return ipnet.IP
				}
			}
		}
	}
	return nil
}
