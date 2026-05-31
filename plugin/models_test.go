package plugin

import (
	"encoding/json"
	"testing"
)

func toMap(t *testing.T, v interface{}) map[string]interface{} {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var m map[string]interface{}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	return m
}

func TestServiceInfo_SerialisesCamelCaseWithMethodArray(t *testing.T) {
	svc := NewServiceInfo("DemoService").
		WithMethods(UnaryMethod("Echo")).
		WithDescription("A demo service")

	m := toMap(t, svc)
	if m["name"] != "DemoService" {
		t.Errorf("want name=DemoService, got %v", m["name"])
	}
	if m["description"] != "A demo service" {
		t.Errorf("want description set, got %v", m["description"])
	}
	methods := m["methods"].([]interface{})
	first := methods[0].(map[string]interface{})
	if first["name"] != "Echo" {
		t.Errorf("want method name=Echo, got %v", first["name"])
	}
	if first["methodType"] != "Unary" {
		t.Errorf("want methodType=Unary, got %v", first["methodType"])
	}
}

func TestServiceInfo_OmitsDescriptionWhenEmpty(t *testing.T) {
	m := toMap(t, NewServiceInfo("S"))
	if _, ok := m["description"]; ok {
		t.Errorf("description should be omitted when empty")
	}
}

func TestMethodInfo_UnaryIsNotStreaming(t *testing.T) {
	mi := UnaryMethod("Get")
	if mi.ClientStreaming || mi.ServerStreaming {
		t.Errorf("unary should not be streaming")
	}
	if mi.MethodType != MethodTypeUnary {
		t.Errorf("want Unary, got %v", mi.MethodType)
	}
}

func TestMethodInfo_ServerStreamingSetsFlagAndMarker(t *testing.T) {
	mi := ServerStreamingMethod("Watch")
	if mi.ClientStreaming {
		t.Errorf("server-streaming should not flip client flag")
	}
	if !mi.ServerStreaming {
		t.Errorf("server-streaming should flip server flag")
	}
	if mi.MethodType != MethodTypeServerStreaming {
		t.Errorf("want ServerStreaming, got %v", mi.MethodType)
	}
}

func TestMethodInfo_BidirectionalFlipsBothFlags(t *testing.T) {
	mi := BidirectionalMethod("Chat")
	if !mi.ClientStreaming || !mi.ServerStreaming {
		t.Errorf("bidirectional should flip both flags")
	}
}

func TestMethodInfo_ChainsInputOutputSummary(t *testing.T) {
	mi := UnaryMethod("Echo").
		WithInput(NewMessageInfo("Req", "echo.Req")).
		WithOutput(NewMessageInfo("Resp", "echo.Resp")).
		WithSummary("Echo back")
	if mi.InputType == nil || mi.OutputType == nil {
		t.Errorf("input/output should be set")
	}
	if mi.Summary != "Echo back" {
		t.Errorf("want summary set, got %q", mi.Summary)
	}
}

func TestMethodInfo_OmitsUnsetOptionalFields(t *testing.T) {
	m := toMap(t, UnaryMethod("X"))
	for _, key := range []string{"inputType", "outputType", "httpMethod", "httpPath", "summary"} {
		if _, ok := m[key]; ok {
			t.Errorf("%s should be omitted when unset", key)
		}
	}
}

func TestFieldInfo_BuildersPickExpectedTypeNames(t *testing.T) {
	if String("a").TypeName != "string" {
		t.Errorf("string builder wrong typeName")
	}
	if Int32("b").TypeName != "int32" {
		t.Errorf("int32 builder wrong typeName")
	}
	if Bool("c").TypeName != "bool" {
		t.Errorf("bool builder wrong typeName")
	}
	if !String("a").MarkRequired().Required {
		t.Errorf("MarkRequired should flip flag")
	}
}

func TestMessageInfo_WithFieldsExtendsList(t *testing.T) {
	m := NewMessageInfo("M", "ns.M").WithFields(
		String("name").MarkRequired(),
		Int32("count"),
	)
	if len(m.Fields) != 2 {
		t.Errorf("want 2 fields, got %d", len(m.Fields))
	}
	if !m.Fields[0].Required {
		t.Errorf("first field should be required")
	}
	if m.Fields[1].Required {
		t.Errorf("second field should not be required")
	}
}

func TestInvokeResult_OKAndErr(t *testing.T) {
	ok := NewOKResult(`{"echoed":true}`)
	if ok.Status != "OK" {
		t.Errorf("want status OK, got %q", ok.Status)
	}
	if ok.Response != `{"echoed":true}` {
		t.Errorf("response mismatch")
	}
	withMsg := NewErrResult("Error", "nope")
	if withMsg.Status != "Error" || withMsg.Response != "nope" {
		t.Errorf("err result fields wrong")
	}
}

func TestInvokeResult_SerialisesCamelCaseWithMetadata(t *testing.T) {
	m := toMap(t, NewOKResult("{}"))
	if _, ok := m["durationMs"]; !ok {
		t.Errorf("durationMs should be present")
	}
	if _, ok := m["metadata"]; !ok {
		t.Errorf("metadata should be present")
	}
}

func TestPluginSetting_RoundTripsDefaultAndRequired(t *testing.T) {
	s := NewSetting("host", "Host", "string").
		WithDefault("localhost").
		MarkRequired()
	m := toMap(t, s)
	if m["key"] != "host" {
		t.Errorf("key wrong")
	}
	if m["defaultValue"] != "localhost" {
		t.Errorf("defaultValue wrong")
	}
	if m["required"] != true {
		t.Errorf("required should round-trip true")
	}
}
