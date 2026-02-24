package protocol

type Message struct {
	Type    string            `json:"type"`
	Sender  string            `json:"sender"`
	Payload map[string]string `json:"payload"`
}
