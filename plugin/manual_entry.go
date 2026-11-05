package plugin

import (
	"fmt"
	"regexp"
)

// ManualEntryPlugin is the optional capability for a hand-entry form
// (Kuestenlogik/Bowire#1003): how a call to the plugin is entered by hand,
// without discovery. The form goes to the host in the initialize reply as
// manualEntry; the workbench renders a Compose form from it and offers
// exactly these method types in its freeform form. Return nil for no form.
// The wire shape is the Bowire repo's docs/architecture/sidecar-plugins.md,
// "A hand-entry form".
type ManualEntryPlugin interface {
	ManualEntry() *ManualEntryForm
}

// ManualEntryForm is how a call to the plugin is entered by hand.
type ManualEntryForm struct {
	CallKinds      []ManualCallKind `json:"callKinds"`
	Fields         []ManualField    `json:"fields"`
	URLPlaceholder string           `json:"urlPlaceholder,omitempty"`
}

// ManualCallKind is one kind of call the plugin offers, e.g. produce or
// consume. MethodType is "Unary" (invoke), "ServerStreaming" (invokeStream),
// "ClientStreaming" or "Duplex" (a channel). Service and Method are
// templates: {key} is the value of the field with that key.
type ManualCallKind struct {
	ID          string `json:"id"`
	Label       string `json:"label,omitempty"`
	MethodType  string `json:"methodType"`
	Service     string `json:"service"`
	Method      string `json:"method"`
	Description string `json:"description,omitempty"`
	BodyExample string `json:"bodyExample,omitempty"`
}

// ManualField is one field of the form. Placement "template" feeds only the
// call kinds' templates; "metadata" (the default when empty) puts the value
// into the call's metadata under Key. CallKinds limits the field to those
// kinds; empty means all. Type is "string" (default), "number", "bool" or
// "select" (with Options).
type ManualField struct {
	Key          string          `json:"key"`
	Label        string          `json:"label,omitempty"`
	Placement    string          `json:"placement,omitempty"`
	Description  string          `json:"description,omitempty"`
	Placeholder  string          `json:"placeholder,omitempty"`
	Required     bool            `json:"required"`
	Type         string          `json:"type,omitempty"`
	Options      []SettingOption `json:"options,omitempty"`
	DefaultValue string          `json:"defaultValue,omitempty"`
	CallKinds    []string        `json:"callKinds,omitempty"`
}

var manualMethodTypes = map[string]bool{
	"Unary": true, "ServerStreaming": true, "ClientStreaming": true, "Duplex": true,
}

var manualPlaceholder = regexp.MustCompile(`\{([^{}]+)\}`)

// ManualEntryProblems reports what is wrong with a form, or nothing — the
// check the host runs before it serves one. The dispatcher refuses a form
// with a problem at initialize, so the plugin's author sees it rather than a
// form that silently never appears.
func ManualEntryProblems(form ManualEntryForm) []string {
	var problems []string
	if len(form.CallKinds) == 0 {
		problems = append(problems, "it offers no call kind")
	}

	kindIDs := map[string]bool{}
	for _, k := range form.CallKinds {
		switch {
		case k.ID == "":
			problems = append(problems, "a call kind has no id")
		case kindIDs[k.ID]:
			problems = append(problems, fmt.Sprintf("call kind '%s' is declared twice", k.ID))
		default:
			kindIDs[k.ID] = true
		}
		if !manualMethodTypes[k.MethodType] {
			problems = append(problems, fmt.Sprintf("call kind '%s' has method type '%s'", k.ID, k.MethodType))
		}
	}

	fieldKeys := map[string]bool{}
	for _, f := range form.Fields {
		switch {
		case f.Key == "":
			problems = append(problems, "a field has no key")
		case fieldKeys[f.Key]:
			problems = append(problems, fmt.Sprintf("field '%s' is declared twice", f.Key))
		default:
			fieldKeys[f.Key] = true
		}
		if f.Type == "select" && len(f.Options) == 0 {
			problems = append(problems, fmt.Sprintf("select field '%s' has no options", f.Key))
		}
		for _, id := range f.CallKinds {
			if !kindIDs[id] {
				problems = append(problems, fmt.Sprintf("field '%s' names unknown call kind '%s'", f.Key, id))
			}
		}
	}

	for _, k := range form.CallKinds {
		for _, template := range []string{k.Service, k.Method} {
			for _, m := range manualPlaceholder.FindAllStringSubmatch(template, -1) {
				if !fieldKeys[m[1]] {
					problems = append(problems, fmt.Sprintf(
						"call kind '%s' uses the placeholder '%s', which no field declares", k.ID, m[1]))
				}
			}
		}
	}
	return problems
}
