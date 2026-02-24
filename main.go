package main

import (
	"flag"

	"github.com/priyansh-narang2308/gizzard/master"
	"github.com/priyansh-narang2308/gizzard/node"
)

func main() {

	role := flag.String("role", "", "master or node")
	id := flag.String("id", "", "node id")
	port := flag.String("port", "", "port")
	masterAddr := flag.String("master", "", "master address")

	flag.Parse()

	if *role == "master" {
		m := master.NewMaster()
		go m.StartTCP(*port)
		m.StartHTTP("8080")
	}

	if *role == "node" {
		n := node.Node{
			ID:     *id,
			Master: *masterAddr,
			Port:   *port,
		}
		n.Start()
	}
}
