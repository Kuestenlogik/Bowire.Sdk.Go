// Package plugin is the Go SDK for authoring Bowire sidecar plugins.
//
// The wire-side data models are camelCase on the JSON-RPC envelope —
// matches what the Python, Rust, and Node SDKs serialise. Use the
// constructors and `With*` chainable builders to assemble a
// ServiceInfo tree in DiscoverAsync.
package plugin

// MethodType mirrors the wire enum the host expects.
type MethodType string

const (
	MethodTypeUnary           MethodType = "Unary"
	MethodTypeServerStreaming MethodType = "ServerStreaming"
	MethodTypeClientStreaming MethodType = "ClientStreaming"
	MethodTypeBidirectional   MethodType = "Bidirectional"
)

// FieldInfo describes a single field of a MessageInfo. `omitempty`
// on Description matches the wire convention the host's
// deserialiser tolerates (no null key).
type FieldInfo struct {
	Name        string `json:"name"`
	TypeName    string `json:"typeName"`
	Required    bool   `json:"required"`
	Description string `json:"description,omitempty"`
}

// String builds a string-typed FieldInfo.
func String(name string) FieldInfo { return FieldInfo{Name: name, TypeName: "string"} }

// Int32 builds an int32-typed FieldInfo.
func Int32(name string) FieldInfo { return FieldInfo{Name: name, TypeName: "int32"} }

// Int64 builds an int64-typed FieldInfo.
func Int64(name string) FieldInfo { return FieldInfo{Name: name, TypeName: "int64"} }

// Bool builds a bool-typed FieldInfo.
func Bool(name string) FieldInfo { return FieldInfo{Name: name, TypeName: "bool"} }

// Double builds a double-typed FieldInfo.
func Double(name string) FieldInfo { return FieldInfo{Name: name, TypeName: "double"} }

// MarkRequired flips the required flag and returns the modified copy.
func (f FieldInfo) MarkRequired() FieldInfo {
	f.Required = true
	return f
}

// WithDescription sets the field description and returns the modified copy.
func (f FieldInfo) WithDescription(d string) FieldInfo {
	f.Description = d
	return f
}

// MessageInfo describes an input or output message type for a method.
type MessageInfo struct {
	Name        string      `json:"name"`
	FullName    string      `json:"fullName"`
	Fields      []FieldInfo `json:"fields"`
	Description string      `json:"description,omitempty"`
}

// NewMessageInfo builds a MessageInfo with empty fields.
func NewMessageInfo(name, fullName string) MessageInfo {
	return MessageInfo{Name: name, FullName: fullName, Fields: []FieldInfo{}}
}

// WithFields appends the given fields and returns the modified copy.
func (m MessageInfo) WithFields(fields ...FieldInfo) MessageInfo {
	m.Fields = append(m.Fields, fields...)
	return m
}

// WithDescription sets the description and returns the modified copy.
func (m MessageInfo) WithDescription(d string) MessageInfo {
	m.Description = d
	return m
}

// MethodInfo describes a single RPC method.
type MethodInfo struct {
	Name            string       `json:"name"`
	MethodType      MethodType   `json:"methodType"`
	ClientStreaming bool         `json:"clientStreaming"`
	ServerStreaming bool         `json:"serverStreaming"`
	InputType       *MessageInfo `json:"inputType,omitempty"`
	OutputType      *MessageInfo `json:"outputType,omitempty"`
	Summary         string       `json:"summary,omitempty"`
	HTTPMethod      string       `json:"httpMethod,omitempty"`
	HTTPPath        string       `json:"httpPath,omitempty"`
}

// UnaryMethod builds a unary MethodInfo.
func UnaryMethod(name string) MethodInfo {
	return MethodInfo{Name: name, MethodType: MethodTypeUnary}
}

// ServerStreamingMethod builds a server-streaming MethodInfo.
func ServerStreamingMethod(name string) MethodInfo {
	return MethodInfo{Name: name, MethodType: MethodTypeServerStreaming, ServerStreaming: true}
}

// ClientStreamingMethod builds a client-streaming MethodInfo.
func ClientStreamingMethod(name string) MethodInfo {
	return MethodInfo{Name: name, MethodType: MethodTypeClientStreaming, ClientStreaming: true}
}

// BidirectionalMethod builds a bidirectional MethodInfo.
func BidirectionalMethod(name string) MethodInfo {
	return MethodInfo{Name: name, MethodType: MethodTypeBidirectional, ClientStreaming: true, ServerStreaming: true}
}

// WithInput sets the input type.
func (m MethodInfo) WithInput(in MessageInfo) MethodInfo {
	m.InputType = &in
	return m
}

// WithOutput sets the output type.
func (m MethodInfo) WithOutput(out MessageInfo) MethodInfo {
	m.OutputType = &out
	return m
}

// WithSummary sets the summary text shown on the workbench.
func (m MethodInfo) WithSummary(s string) MethodInfo {
	m.Summary = s
	return m
}

// WithHTTP sets the HTTP method and path for transports that map to HTTP (REST, Connect).
func (m MethodInfo) WithHTTP(method, path string) MethodInfo {
	m.HTTPMethod = method
	m.HTTPPath = path
	return m
}

// ServiceInfo is the top-level shape DiscoverAsync returns.
type ServiceInfo struct {
	Name        string       `json:"name"`
	Methods     []MethodInfo `json:"methods"`
	Description string       `json:"description,omitempty"`
}

// NewServiceInfo builds a ServiceInfo with no methods.
func NewServiceInfo(name string) ServiceInfo {
	return ServiceInfo{Name: name, Methods: []MethodInfo{}}
}

// WithMethods appends methods and returns the modified copy.
func (s ServiceInfo) WithMethods(methods ...MethodInfo) ServiceInfo {
	s.Methods = append(s.Methods, methods...)
	return s
}

// WithDescription sets the description.
func (s ServiceInfo) WithDescription(d string) ServiceInfo {
	s.Description = d
	return s
}

// InvokeResult is the return shape of Invoke.
type InvokeResult struct {
	Status     string            `json:"status"`
	Response   string            `json:"response,omitempty"`
	DurationMs int64             `json:"durationMs"`
	Metadata   map[string]string `json:"metadata"`
}

// NewOKResult builds an OK InvokeResult carrying the given response body.
func NewOKResult(response string) InvokeResult {
	return InvokeResult{Status: "OK", Response: response, Metadata: map[string]string{}}
}

// NewErrResult builds an error InvokeResult. Pass an empty response if there's no body to attach.
func NewErrResult(status, response string) InvokeResult {
	return InvokeResult{Status: status, Response: response, Metadata: map[string]string{}}
}

// WithDurationMs records how long the invocation took.
func (r InvokeResult) WithDurationMs(ms int64) InvokeResult {
	r.DurationMs = ms
	return r
}

// WithMetadata merges the given metadata into the result. The wire
// shape is a flat map[string]string.
func (r InvokeResult) WithMetadata(md map[string]string) InvokeResult {
	if r.Metadata == nil {
		r.Metadata = map[string]string{}
	}
	for k, v := range md {
		r.Metadata[k] = v
	}
	return r
}

// PluginSetting is a single user-configurable setting the plugin exposes through Settings().
type PluginSetting struct {
	Key          string      `json:"key"`
	Label        string      `json:"label"`
	Type         string      `json:"type"`
	DefaultValue interface{} `json:"defaultValue,omitempty"`
	Description  string      `json:"description,omitempty"`
	Required     bool        `json:"required"`
}

// NewSetting builds a PluginSetting with the given key/label/type.
func NewSetting(key, label, typ string) PluginSetting {
	return PluginSetting{Key: key, Label: label, Type: typ}
}

// WithDefault sets the default value (any JSON-serialisable type).
func (p PluginSetting) WithDefault(v interface{}) PluginSetting {
	p.DefaultValue = v
	return p
}

// WithDescription sets the description.
func (p PluginSetting) WithDescription(d string) PluginSetting {
	p.Description = d
	return p
}

// MarkRequired flips the required flag and returns the modified copy.
func (p PluginSetting) MarkRequired() PluginSetting {
	p.Required = true
	return p
}
