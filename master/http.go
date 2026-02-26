package master

import (
    "encoding/json"
    "log"
    "net"
    "net/http"
    "strconv"
    "text/template"
)

func (m *Master) StartHTTP(port string) {
	http.HandleFunc("/", m.dashboard)
	http.HandleFunc("/api", m.apiStatus)
	http.HandleFunc("/data", m.dataHandler)
	log.Printf("[DASHBOARD] Running on port %s\n", port)
	if err := http.ListenAndServe("0.0.0.0:"+port, nil); err != nil {
		log.Printf("[ERROR] HTTP server failed: %v\n", err)
	}
}

// Handles POST requests for data operations (PUT)
func (m *Master) dataHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	var req struct {
		Type    string            `json:"type"`
		Payload map[string]string `json:"payload"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}
	if req.Type != "PUT" {
		http.Error(w, "Unsupported operation", http.StatusBadRequest)
		return
	}
	shardID := req.Payload["shard"]
	key := req.Payload["key"]
	value := req.Payload["value"]
	// Find leader node for shard
	m.Mu.Lock()
	var leaderAddr string
	for _, s := range m.Shards {
		if strconv.Itoa(s.ID) == shardID {
			leaderAddr = s.Leader
			break
		}
	}
	m.Mu.Unlock()
	if leaderAddr == "" {
		http.Error(w, "Shard leader not found", http.StatusNotFound)
		return
	}
	// Forward PUT to leader node
	nodeConn, err := net.Dial("tcp", leaderAddr)
	if err != nil {
		http.Error(w, "Failed to connect to node", http.StatusBadGateway)
		return
	}
	defer nodeConn.Close()
	msg := map[string]interface{}{
		"type": "PUT",
		"payload": map[string]string{
			"shard": shardID,
			"key":   key,
			"value": value,
		},
	}
	if err := json.NewEncoder(nodeConn).Encode(msg); err != nil {
		http.Error(w, "Failed to send to node", http.StatusInternalServerError)
		return
	}
	var nodeResp map[string]interface{}
	if err := json.NewDecoder(nodeConn).Decode(&nodeResp); err != nil {
		http.Error(w, "Failed to read node response", http.StatusInternalServerError)
		return
	}
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(nodeResp)
    }
    
    // Handles GET requests for the dashboard
    func (m *Master) dashboard(w http.ResponseWriter, r *http.Request) {
        m.Mu.Lock()
        defer m.Mu.Unlock()
        
        tmpl, err := template.New("dashboard").Parse(dashboardHTML)
        if err != nil {
            http.Error(w, "Failed to parse template", http.StatusInternalServerError)
            return
        }
        
        w.Header().Set("Content-Type", "text/html")
        tmpl.Execute(w, m)
    }
    
    // Handles API status requests
    func (m *Master) apiStatus(w http.ResponseWriter, r *http.Request) {
        m.Mu.Lock()
        defer m.Mu.Unlock()
        
        w.Header().Set("Content-Type", "application/json")
        json.NewEncoder(w).Encode(map[string]interface{}{
            "nodes":  m.Nodes,
            "shards": m.Shards,
        })
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
                    <span><strong>Shard {{.ID}}</strong> <br><small style="color:#666;">Keys stored: {{.Keys}}</small></span>
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

