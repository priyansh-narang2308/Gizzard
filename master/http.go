package master

import (
	"encoding/json"
	"fmt"
	"net/http"
)

func (m *Master) StartHTTP(port string) {
	http.HandleFunc("/", m.dashboard)
	http.HandleFunc("/api", m.apiStatus)

	fmt.Println("Dashboard running on port", port)
	http.ListenAndServe(":"+port, nil)
}

func (m *Master) dashboard(w http.ResponseWriter, r *http.Request) {
	html := "<h1>Gizzard Cluster Dashboard</h1>"

	html += "<h2>Nodes</h2><ul>"
	for _, node := range m.Nodes {
		html += fmt.Sprintf("<li>%s - %s - %s</li>",
			node.ID, node.Address, node.Status)
	}
	html += "</ul>"

	html += "<h2>Shards</h2><ul>"
	for _, shard := range m.Shards {
		html += fmt.Sprintf("<li>Shard %d → Leader %s</li>",
			shard.ID, shard.Leader)
	}
	html += "</ul>"

	w.Write([]byte(html))
}

func (m *Master) apiStatus(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(m)
}