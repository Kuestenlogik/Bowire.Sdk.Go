package plugin

import (
	"encoding/json"
	"math"
	"sort"
	"strconv"
	"strings"
)

// SettingValues are the values behind the settings a plugin declared
// (Kuestenlogik/Bowire#693), as the host sent them with one call.
//
// The host reads its settings store on the plugin's behalf and puts a
// `settings` object on the params of discover, invoke, invokeStream and
// openChannel. A fresh value arrives with every call, so a changed setting
// takes effect without restarting the sidecar. Two rules of that wire shape
// decide how the accessors behave:
//
//   - Values are strings whatever the declared type. The accessors parse
//     what the setting declared.
//   - Only keys that are actually set travel. An absent key means "use the
//     default you declared", so the accessors fall back to the declaration's
//     DefaultValue, then to the fallback the caller passes.
//
// A value that does not parse as the requested type counts as absent: the
// declared default applies. A hand-edited settings file should not take the
// plugin down.
//
// The zero value is usable and holds nothing.
type SettingValues struct {
	values   map[string]string
	declared map[string]PluginSetting
}

// NewSettingValues builds SettingValues from already-decoded values and the
// plugin's declarations. Plugins rarely need it outside their tests: the
// dispatcher builds one per call.
func NewSettingValues(values map[string]string, declared []PluginSetting) SettingValues {
	v := SettingValues{values: map[string]string{}, declared: map[string]PluginSetting{}}
	for k, s := range values {
		v.values[k] = s
	}
	for _, d := range declared {
		v.declared[d.Key] = d
	}
	return v
}

// settingValuesFromWire decodes the raw `settings` param. Anything but an
// object is "nothing set"; a number or boolean is taken as its text, since
// the host's store tolerates one in a hand-edited file too.
func settingValuesFromWire(raw json.RawMessage, declared []PluginSetting) SettingValues {
	values := map[string]string{}
	var obj map[string]json.RawMessage
	if len(raw) > 0 && json.Unmarshal(raw, &obj) == nil {
		for k, r := range obj {
			// null decodes into a string without an error; it means "not set".
			if string(r) == "null" {
				continue
			}
			var s string
			if json.Unmarshal(r, &s) == nil {
				values[k] = s
				continue
			}
			var b bool
			if json.Unmarshal(r, &b) == nil {
				values[k] = strconv.FormatBool(b)
				continue
			}
			var n json.Number
			if json.Unmarshal(r, &n) == nil {
				values[k] = n.String()
			}
		}
	}
	return NewSettingValues(values, declared)
}

// Has reports whether the host sent a value for key with this call.
func (v SettingValues) Has(key string) bool {
	_, ok := v.values[key]
	return ok
}

// Raw returns the text the host sent for key, and whether it sent one. No
// default applies.
func (v SettingValues) Raw(key string) (string, bool) {
	s, ok := v.values[key]
	return s, ok
}

// Keys lists the keys the host sent a value for, sorted.
func (v SettingValues) Keys() []string {
	keys := make([]string, 0, len(v.values))
	for k := range v.values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// String returns the value as text: the value sent, else the declared
// default, else fallback.
func (v SettingValues) String(key, fallback string) string {
	return resolve(v, key, fallback, parseString)
}

// Bool returns the value as a boolean: "true" / "false" (any case), "1" /
// "0". Falls back like String.
func (v SettingValues) Bool(key string, fallback bool) bool {
	return resolve(v, key, fallback, parseBool)
}

// Number returns the value as a finite float64. Falls back like String.
func (v SettingValues) Number(key string, fallback float64) float64 {
	return resolve(v, key, fallback, parseNumber)
}

// Int returns the value as an int; a number with a fraction does not parse.
// Falls back like String.
func (v SettingValues) Int(key string, fallback int) int {
	return resolve(v, key, fallback, parseInt)
}

// MarshalJSON writes the values the host sent, as the wire carries them.
func (v SettingValues) MarshalJSON() ([]byte, error) {
	if v.values == nil {
		return []byte("{}"), nil
	}
	return json.Marshal(v.values)
}

func resolve[T any](v SettingValues, key string, fallback T, parse func(interface{}) (T, bool)) T {
	if sent, ok := v.values[key]; ok {
		if parsed, ok := parse(sent); ok {
			return parsed
		}
	}
	if d, ok := v.declared[key]; ok && d.DefaultValue != nil {
		if parsed, ok := parse(d.DefaultValue); ok {
			return parsed
		}
	}
	return fallback
}

func parseString(value interface{}) (string, bool) {
	switch x := value.(type) {
	case string:
		return x, true
	case bool:
		return strconv.FormatBool(x), true
	case float64, float32, int, int32, int64:
		return strings.TrimSpace(strconv.FormatFloat(toFloat(x), 'f', -1, 64)), true
	}
	return "", false
}

func parseBool(value interface{}) (bool, bool) {
	switch x := value.(type) {
	case bool:
		return x, true
	case string:
		switch strings.ToLower(strings.TrimSpace(x)) {
		case "true", "1":
			return true, true
		case "false", "0":
			return false, true
		}
	}
	return false, false
}

func parseNumber(value interface{}) (float64, bool) {
	switch x := value.(type) {
	case string:
		s := strings.TrimSpace(x)
		if s == "" {
			return 0, false
		}
		n, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsInf(n, 0) || math.IsNaN(n) {
			return 0, false
		}
		return n, true
	case float64, float32, int, int32, int64:
		n := toFloat(x)
		if math.IsInf(n, 0) || math.IsNaN(n) {
			return 0, false
		}
		return n, true
	}
	return 0, false
}

func parseInt(value interface{}) (int, bool) {
	n, ok := parseNumber(value)
	if !ok || n != math.Trunc(n) || n > math.MaxInt32 || n < math.MinInt32 {
		return 0, false
	}
	return int(n), true
}

func toFloat(x interface{}) float64 {
	switch n := x.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	}
	return math.NaN()
}
