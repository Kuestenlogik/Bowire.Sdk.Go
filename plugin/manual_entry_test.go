package plugin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

// The hand-entry form a plugin declares (Kuestenlogik/Bowire#1003): it lands
// in the initialize reply as manualEntry, absent when the plugin declares
// none, and a form the host would refuse fails the handshake with the reason.

type formPlugin struct {
	testPlugin
	form *ManualEntryForm
}

func (p formPlugin) ManualEntry() *ManualEntryForm { return p.form }

func brokerForm() *ManualEntryForm {
	return &ManualEntryForm{
		URLPlaceholder: "mybroker://localhost:9000",
		CallKinds: []ManualCallKind{
			{ID: "produce", Label: "Produce", MethodType: "Unary", Service: "{topic}", Method: "produce"},
			{ID: "consume", MethodType: "ServerStreaming", Service: "{topic}", Method: "consume"},
		},
		Fields: []ManualField{
			{Key: "topic", Label: "Topic", Placement: "template", Required: true},
			{Key: "priority", Type: "select", CallKinds: []string{"produce"},
				Options: []SettingOption{{Value: "high", Label: "High"}, {Value: "low"}}},
		},
	}
}

func initialize(t *testing.T, p BowirePlugin) Response {
	t.Helper()
	r := NewDispatcher(p, discardNotify).Dispatch(context.Background(),
		Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: "initialize"})
	reply, ok := r.(ReplyResult)
	if !ok {
		t.Fatalf("want ReplyResult, got %T", r)
	}
	return reply.Response
}

func TestManualEntry_TheDeclaredFormIsInTheInitializeReply(t *testing.T) {
	resp := initialize(t, formPlugin{form: brokerForm()})
	if resp.Error != nil {
		t.Fatalf("want a reply, got error %v", resp.Error)
	}

	// Read it back the way the host does: as JSON.
	raw, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		ManualEntry struct {
			URLPlaceholder string `json:"urlPlaceholder"`
			CallKinds      []struct {
				ID, MethodType, Service, Method string
			} `json:"callKinds"`
			Fields []struct {
				Key, Placement string
				Required       bool
				CallKinds      []string `json:"callKinds"`
			} `json:"fields"`
		} `json:"manualEntry"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	me := wire.ManualEntry
	if me.URLPlaceholder != "mybroker://localhost:9000" || len(me.CallKinds) != 2 || len(me.Fields) != 2 {
		t.Fatalf("unexpected form on the wire: %s", raw)
	}
	if k := me.CallKinds[0]; k.ID != "produce" || k.MethodType != "Unary" || k.Service != "{topic}" {
		t.Errorf("first call kind: %+v", k)
	}
	if f := me.Fields[0]; f.Key != "topic" || f.Placement != "template" || !f.Required {
		t.Errorf("first field: %+v", f)
	}
	if got := me.Fields[1].CallKinds; len(got) != 1 || got[0] != "produce" {
		t.Errorf("second field's call kinds: %v", got)
	}
}

func TestManualEntry_NoFormMeansNoManualEntryInTheReply(t *testing.T) {
	for name, p := range map[string]BowirePlugin{
		"no capability": &testPlugin{},
		"nil form":      formPlugin{},
	} {
		result := initialize(t, p).Result.(map[string]interface{})
		if _, ok := result["manualEntry"]; ok {
			t.Errorf("%s: want no manualEntry, got %v", name, result["manualEntry"])
		}
	}
}

func TestManualEntry_AFormTheHostWouldRefuseFailsTheHandshake(t *testing.T) {
	broken := &ManualEntryForm{CallKinds: []ManualCallKind{{ID: "produce", MethodType: "Unary", Service: "{topic}"}}}

	resp := initialize(t, formPlugin{form: broken})

	if resp.Error == nil || resp.Error.Code != ErrInternal {
		t.Fatalf("want an internal error, got %+v", resp)
	}
	if !strings.Contains(resp.Error.Message, "placeholder 'topic'") {
		t.Errorf("want the reason in the message, got %q", resp.Error.Message)
	}
}

func TestManualEntryProblems_NamesEachProblem(t *testing.T) {
	cases := map[string]ManualEntryForm{
		"it offers no call kind": {},
		"call kind 'a' has method type 'Sometimes'": {
			CallKinds: []ManualCallKind{{ID: "a", MethodType: "Sometimes"}}},
		"call kind 'a' is declared twice": {
			CallKinds: []ManualCallKind{{ID: "a", MethodType: "Unary"}, {ID: "a", MethodType: "Unary"}}},
		"select field 'v' has no options": {
			CallKinds: []ManualCallKind{{ID: "a", MethodType: "Unary"}},
			Fields:    []ManualField{{Key: "v", Type: "select"}}},
		"field 'w' names unknown call kind 'b'": {
			CallKinds: []ManualCallKind{{ID: "a", MethodType: "Unary"}},
			Fields:    []ManualField{{Key: "w", CallKinds: []string{"b"}}}},
	}
	for want, form := range cases {
		found := false
		for _, p := range ManualEntryProblems(form) {
			if p == want {
				found = true
			}
		}
		if !found {
			t.Errorf("want %q among %v", want, ManualEntryProblems(form))
		}
	}
	if p := ManualEntryProblems(*brokerForm()); len(p) != 0 {
		t.Errorf("a sound form has no problems, got %v", p)
	}
}
