package plugin

import "encoding/json"

// Request is the JSON-RPC 2.0 request envelope. ID is left as
// json.RawMessage so we can echo whatever shape the host sent
// (string, number, or null) without losing fidelity.
type Request struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id,omitempty"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Response is the JSON-RPC 2.0 reply envelope.
type Response struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Result  interface{}     `json:"result,omitempty"`
	Error   *ErrorObject    `json:"error,omitempty"`
}

// Notification is the JSON-RPC 2.0 notification envelope (no ID).
type Notification struct {
	JSONRPC string      `json:"jsonrpc"`
	Method  string      `json:"method"`
	Params  interface{} `json:"params,omitempty"`
}

// ErrorObject is the JSON-RPC 2.0 error shape.
type ErrorObject struct {
	Code    int         `json:"code"`
	Message string      `json:"message"`
	Data    interface{} `json:"data,omitempty"`
}

// Standard JSON-RPC 2.0 error codes the dispatcher emits.
const (
	ErrMethodNotFound = -32601
	ErrInvalidParams  = -32602
	ErrInternal       = -32603
)

// nullID returns the `null` JSON literal as a RawMessage — used when
// the host sent a request without an ID and we still need to emit
// a well-formed response.
func nullID() json.RawMessage { return json.RawMessage("null") }

func okResponse(id json.RawMessage, result interface{}) Response {
	if id == nil {
		id = nullID()
	}
	return Response{JSONRPC: "2.0", ID: id, Result: result}
}

func errResponse(id json.RawMessage, code int, message string) Response {
	if id == nil {
		id = nullID()
	}
	return Response{JSONRPC: "2.0", ID: id, Error: &ErrorObject{Code: code, Message: message}}
}
