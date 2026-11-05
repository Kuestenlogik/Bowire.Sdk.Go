package plugin

import (
	"context"
	"encoding/json"
	"testing"
)

// settingsPlugin declares settings and records what each call received.
type settingsPlugin struct {
	discovered SettingValues
	invoked    SettingValues
	opened     SettingValues
}

func (*settingsPlugin) ID() string   { return "settings" }
func (*settingsPlugin) Name() string { return "Settings" }

func (*settingsPlugin) Settings() []PluginSetting {
	return []PluginSetting{
		NewSetting("mode", "Mode", "select").WithDefault("a").WithOptions(
			SettingOption{Value: "a", Label: "A"},
			SettingOption{Value: "b"},
		),
		NewSetting("count", "Count", "number").WithDefault(3),
		NewSetting("verbose", "Verbose", "bool").WithDefault(false),
		NewSetting("label", "Label", "string"),
	}
}

func (p *settingsPlugin) Discover(_ context.Context, req DiscoverRequest) ([]ServiceInfo, error) {
	p.discovered = req.Settings
	return nil, nil
}

func (p *settingsPlugin) Invoke(_ context.Context, req InvokeRequest) (InvokeResult, error) {
	p.invoked = req.Settings
	return NewOKResult("{}"), nil
}

func (p *settingsPlugin) OpenChannel(_ context.Context, req ChannelRequest, _ ChannelHandle) error {
	p.opened = req.Settings
	return nil
}

func callWithSettings(t *testing.T, d *Dispatcher, method string, params map[string]interface{}) Response {
	t.Helper()
	r := d.Dispatch(context.Background(), Request{JSONRPC: "2.0", ID: mustRawID(t, 1), Method: method, Params: mustRaw(t, params)})
	reply, ok := r.(ReplyResult)
	if !ok {
		t.Fatalf("%s: want ReplyResult, got %T", method, r)
	}
	if reply.Response.Error != nil {
		t.Fatalf("%s: %+v", method, reply.Response.Error)
	}
	return reply.Response
}

// The declarations reach the host with initialize, a select with the
// options it needs; the JSON is what the host's Settings dialog reads.
func TestSettings_InitializeSendsTheDeclarationsWithSelectOptions(t *testing.T) {
	d := NewDispatcher(&settingsPlugin{}, discardNotify)
	resp := callWithSettings(t, d, "initialize", map[string]interface{}{})
	raw, err := json.Marshal(resp.Result)
	if err != nil {
		t.Fatal(err)
	}
	var wire struct {
		Settings []struct {
			Key     string `json:"key"`
			Type    string `json:"type"`
			Options []struct {
				Value string `json:"value"`
				Label string `json:"label"`
			} `json:"options"`
		} `json:"settings"`
	}
	if err := json.Unmarshal(raw, &wire); err != nil {
		t.Fatal(err)
	}
	if len(wire.Settings) != 4 || wire.Settings[0].Key != "mode" || wire.Settings[0].Type != "select" {
		t.Fatalf("want the four declarations, mode first as a select: %s", raw)
	}
	opts := wire.Settings[0].Options
	if len(opts) != 2 || opts[0].Value != "a" || opts[0].Label != "A" || opts[1].Value != "b" {
		t.Errorf("want the select's options on the wire: %s", raw)
	}
	if wire.Settings[1].Options != nil {
		t.Errorf("a setting without options carries no options key: %s", raw)
	}
}

// Each call carries its own values, and the accessors parse them as what
// the setting declared.
func TestSettings_EveryCallReceivesTheValuesSentWithIt(t *testing.T) {
	p := &settingsPlugin{}
	d := NewDispatcher(p, discardNotify)
	sent := map[string]interface{}{"mode": "b", "count": "7", "verbose": "TRUE", "label": "hi"}

	callWithSettings(t, d, "discover", map[string]interface{}{"serverUrl": "x://", "settings": sent})
	callWithSettings(t, d, "invoke", map[string]interface{}{"service": "S", "method": "M", "settings": sent})
	callWithSettings(t, d, "openChannel", map[string]interface{}{"channelId": "c1", "settings": sent})

	for name, v := range map[string]SettingValues{"discover": p.discovered, "invoke": p.invoked, "openChannel": p.opened} {
		if got := v.String("mode", ""); got != "b" {
			t.Errorf("%s: mode = %q", name, got)
		}
		if got := v.Int("count", 0); got != 7 {
			t.Errorf("%s: count = %d", name, got)
		}
		if got := v.Bool("verbose", false); !got {
			t.Errorf("%s: verbose = %v", name, got)
		}
		if got := v.String("label", ""); got != "hi" {
			t.Errorf("%s: label = %q", name, got)
		}
	}

	// The next call sends nothing: the values of the last one do not stick.
	callWithSettings(t, d, "invoke", map[string]interface{}{"service": "S", "method": "M"})
	if p.invoked.Has("mode") || len(p.invoked.Keys()) != 0 {
		t.Errorf("a call without settings saw values: %v", p.invoked.Keys())
	}
	if got := p.invoked.String("mode", "z"); got != "a" {
		t.Errorf("an absent key resolves to the declared default, got %q", got)
	}
}

func TestSettingValues_FallBackToTheDeclaredDefaultThenTheFallback(t *testing.T) {
	declared := (&settingsPlugin{}).Settings()
	v := NewSettingValues(map[string]string{"count": "many", "verbose": "maybe"}, declared)

	if got := v.Int("count", 9); got != 3 {
		t.Errorf("an unparsable value takes the declared default: got %d", got)
	}
	if got := v.Bool("verbose", true); got {
		t.Errorf("an unparsable bool takes the declared default false")
	}
	if got := v.String("label", "fallback"); got != "fallback" {
		t.Errorf("no value, no declared default: the fallback, got %q", got)
	}
	if got := v.Number("undeclared", 1.5); got != 1.5 {
		t.Errorf("an undeclared key takes the fallback, got %v", got)
	}
	if raw, ok := v.Raw("count"); !ok || raw != "many" {
		t.Errorf("Raw returns the text as sent: %q %v", raw, ok)
	}
}

func TestSettingValues_ParseNumbersAndBooleans(t *testing.T) {
	v := NewSettingValues(map[string]string{"a": " 2.5 ", "b": "1", "c": "0", "d": "2.5", "e": "Inf"}, nil)
	if got := v.Number("a", 0); got != 2.5 {
		t.Errorf("a: %v", got)
	}
	if !v.Bool("b", false) || v.Bool("c", true) {
		t.Errorf("1 / 0 read as booleans")
	}
	if got := v.Int("d", -1); got != -1 {
		t.Errorf("a fraction is not an int: %d", got)
	}
	if got := v.Number("e", -1); got != -1 {
		t.Errorf("an infinite number is not a value: %v", got)
	}
}

// A hand-edited settings file may hold a JSON number or boolean; those
// arrive as their text, anything else as nothing.
func TestSettingValues_FromWireTakesPrimitivesAsText(t *testing.T) {
	v := settingValuesFromWire(json.RawMessage(`{"n":4,"b":true,"s":"x","o":{"k":1},"z":null}`), nil)
	if got := v.Keys(); len(got) != 3 || got[0] != "b" || got[1] != "n" || got[2] != "s" {
		t.Errorf("keys: %v", got)
	}
	if got := v.Int("n", 0); got != 4 {
		t.Errorf("n: %d", got)
	}
	if !v.Bool("b", false) {
		t.Errorf("b")
	}
	for _, raw := range []string{`[]`, `"x"`, `null`, ``} {
		if got := settingValuesFromWire(json.RawMessage(raw), nil).Keys(); len(got) != 0 {
			t.Errorf("%q is nothing set, got %v", raw, got)
		}
	}
	if b, _ := json.Marshal(v); string(b) != `{"b":"true","n":"4","s":"x"}` {
		t.Errorf("marshal: %s", b)
	}
	if b, _ := json.Marshal(SettingValues{}); string(b) != `{}` {
		t.Errorf("the zero value marshals as an empty object: %s", b)
	}
}
