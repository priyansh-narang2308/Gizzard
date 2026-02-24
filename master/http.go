package master

import (
	"encoding/json"
	"html/template"
	"log"
	"net/http"
	"sort"
	"strconv"
)

func (m *Master) StartHTTP(port string) {
	http.HandleFunc("/", m.dashboard)
	http.HandleFunc("/api", m.apiStatus)

	log.Printf("[DASHBOARD] Running on port %s\n", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Printf("[ERROR] HTTP server failed: %v\n", err)
	}
}

const dashboardHTML = `
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Gizzard Cluster Dashboard</title>
    <style>
        body {
            font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
            background-color: #f9ecec;
            color: #333;
            max-width: 900px;
            margin: 0 auto;
            padding: 2rem;
            line-height: 1.6;
        }
        h1 {
            border-bottom: 2px solid #ddd;
            padding-bottom: 0.5rem;
            color: #222;
        }
        h2 {
            margin-top: 2rem;
            color: #444;
        }
        .grid {
            display: grid;
            grid-template-columns: 1fr;
            gap: 1.5rem;
        }
        @media (min-width: 600px) {
            .grid { grid-template-columns: 1fr 1fr; }
        }
        .card {
            background: #fff;
            padding: 1.5rem;
            border-radius: 8px;
            box-shadow: 0 2px 5px rgba(0,0,0,0.05);
            border: 1px solid #eee;
        }
        ul {
            list-style: none;
            padding: 0;
            margin: 0;
        }
        li {
            padding: 0.75rem 0;
            border-bottom: 1px solid #f0f0f0;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        li:last-child {
            border-bottom: none;
        }
        .status {
            font-weight: bold;
            font-size: 0.9em;
            padding: 0.2rem 0.6rem;
            border-radius: 4px;
        }
        .alive { background: #e6f4ea; color: #1e8e3e; }
        .dead { background: #fce8e6; color: #d93025; }
        .leader { color: #1a73e8; font-family: monospace; }
        .empty {
            color: #888;
            font-style: italic;
            text-align: center;
            padding: 1rem 0;
        }
    </style>
    <script>
        setTimeout(() => window.location.reload(), 5000); // Auto-refresh every 5 seconds
    </script>
</head>
<body>
    <h1>Gizzard Cluster Dashboard</h1>
    
    <div class="grid">
        <div class="card">
            <h2>Nodes ({{len .Nodes}})</h2>
            {{if .Nodes}}
            <ul>
                {{range .Nodes}}
                <li>
                    <span><strong>Node {{.ID}}</strong> <br><small style="color:#666;">{{.Address}}</small></span>
                    <span class="status {{if eq .Status "ALIVE"}}alive{{else}}dead{{end}}">{{.Status}}</span>
                </li>
                {{end}}
            </ul>
            {{else}}
            <div class="empty">No nodes connected.</div>
            {{end}}
        </div>
        
        <div class="card">
            <h2>Shards ({{len .Shards}})</h2>
            {{if .Shards}}
            <ul>
                {{range .Shards}}
                <li>
                    <span><strong>Shard {{.ID}}</strong></span>
                    <span>Leader: <span class="leader">{{.Leader}}</span></span>
                </li>
                {{end}}
            </ul>
            {{else}}
            <div class="empty">No shards distributed.</div>
            {{end}}
        </div>
    </div>
</body>
</html>
`

func (m *Master) dashboard(w http.ResponseWriter, r *http.Request) {
	tmpl, err := template.New("dashboard").Parse(dashboardHTML)
	if err != nil {
		http.Error(w, "Error rendering dashboard", http.StatusInternalServerError)
		return
	}

	m.Mu.Lock()
	defer m.Mu.Unlock()

	type NodeData struct {
		ID      string
		Address string
		Status  string
	}
	type ShardData struct {
		ID     int
		Leader string
	}

	var nodes []NodeData
	for _, n := range m.Nodes {
		nodes = append(nodes, NodeData{ID: n.ID, Address: n.Address, Status: n.Status})
	}
	sort.Slice(nodes, func(i, j int) bool {
		id1, err1 := strconv.Atoi(nodes[i].ID)
		id2, err2 := strconv.Atoi(nodes[j].ID)
		if err1 == nil && err2 == nil {
			return id1 < id2 // Numeric sort for "1", "2", "10"
		}
		return nodes[i].ID < nodes[j].ID // Fallback to string sort
	})

	var shards []ShardData
	for _, s := range m.Shards {
		shards = append(shards, ShardData{ID: s.ID, Leader: s.Leader})
	}
	sort.Slice(shards, func(i, j int) bool { return shards[i].ID < shards[j].ID })

	data := struct {
		Nodes  []NodeData
		Shards []ShardData
	}{
		Nodes:  nodes,
		Shards: shards,
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	tmpl.Execute(w, data)
}

func (m *Master) apiStatus(w http.ResponseWriter, r *http.Request) {
	json.NewEncoder(w).Encode(m)
}
