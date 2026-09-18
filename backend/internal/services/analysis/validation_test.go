package analysis

import (
	"strings"
	"testing"
	"tree-eclass/internal/integrations/inference"
	"unicode/utf8"
)

func TestDerivedGuidanceNormalization(t *testing.T) {
	p, err := inference.ParseObject(
		"```json\n" + `{"summary":"Δένδρα\u0000","material_type":"exam_guidance","importance":"unknown","topics":["δ","δ",null,{}]}` + "\n```",
	)
	if err != nil {
		t.Fatal(err)
	}
	result, err := validateDocument(p)
	if err != nil || result["summary"] != "Δένδρα" || result["importance"] != "useful" ||
		result["external_material_type"] != "study_guide" ||
		len(result["topics"].([]string)) != 1 {
		t.Fatal(result, err)
	}
	if _, err := validateDocument(map[string]any{"summary": []any{"not a summary"}}); err == nil {
		t.Fatal("invalid summary accepted")
	}
	p = map[string]any{
		"summary":         strings.Repeat("δ", 2000),
		"confidence":      "invented",
		"key_points":      []any{"item", "item"},
		"low_information": "false",
	}
	result, err = validatePage(p)
	if err != nil || utf8.RuneCountInString(result["summary"].(string)) != 1800 || result["confidence"] != "medium" ||
		result["low_information"] != false {
		t.Fatal(result, err)
	}
	for _, raw := range []string{`null`, `[]`, `{"summary":"a"} {"summary":"b"}`, `prefix {"summary":"a"}`} {
		if _, err = inference.ParseObject(raw); err == nil {
			t.Fatal("ambiguous model output accepted", raw)
		}
	}
}
func TestPagePacketAlwaysKeepsTheExactPage(t *testing.T) {
	raw, err := compactPage(
		map[string]any{
			"summary":   strings.Repeat("Δένδρα ", 200),
			"page_type": "diagram",
			"visuals":   []string{strings.Repeat("δ", 1000)},
		},
		301,
		200,
	)
	if err != nil || utf8.RuneCountInString(raw) > 200 || !strings.Contains(raw, `"page":301`) ||
		!strings.Contains(raw, `"details_truncated":true`) {
		t.Fatal(raw, err)
	}
}
