package materials

import (
	"path"
	"regexp"
	"strings"
	"unicode"

	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	"tree-eclass/internal/domain/identity"
)

var analysisTypes = map[string]string{
	"past_exam": "past_paper", "practice_exam": "past_paper", "student_notes": "student_notes",
	"study_guide": "study_guide", "exam_guidance": "study_guide", "textbook": "textbook",
	"assignment": "exercise_solution", "exercise_set": "exercise_solution", "worked_solutions": "exercise_solution",
	"lecture_notes": "lecture_material", "slides": "lecture_material", "tutorial": "lecture_material",
}

var classificationSeparators = regexp.MustCompile(`[_-]+`)

var typeTerms = []struct {
	Type  string
	Terms []string
}{
	{
		"study_guide",
		[]string{
			"study guide",
			"revision",
			"cheat sheet",
			"formula sheet",
			"summary",
			"τυπολογ",
			"σκον",
			"περιληψ",
			"συνοψ",
		},
	},
	{
		"past_paper",
		[]string{
			"exam",
			"past paper",
			"proodos",
			"themata",
			"εξετασ",
			"προοδο",
			"θεμα",
			"june",
			"september",
			"sept",
			"january",
		},
	},
	{"student_notes", []string{"student note", "student-note", "notes", "note", "σημειω"}},
	{"textbook", []string{"textbook", "book", "bishop", "clrs", "burden", "sauer"}},
	{"exercise_solution", []string{"solution", "solutions", "homework", "exercise", "askis", "λυσε", "ασκησ"}},
	{"lecture_material", []string{"lecture", "tutorial", "frontist", "dialex", "διαλεξ", "φροντ"}},
}

func fold(value string) string {
	return strings.Map(func(r rune) rune {
		if unicode.Is(unicode.Mn, r) {
			return -1
		}
		return r
	}, cases.Fold().String(norm.NFKD.String(value)))
}

func classify(source, override string) (string, string) {
	if _, ok := TypeFolders[override]; ok {
		return override, "manual"
	}
	parts := strings.FieldsFunc(identity.Path(source), func(r rune) bool { return r == '/' })
	for i, part := range parts {
		if fold(part) == "external" {
			parts = parts[i+1:]
			break
		}
	}
	if len(parts) > 0 {
		for kind, folder := range TypeFolders {
			if fold(parts[0]) == folder {
				return kind, "folder"
			}
		}
	}
	text := fold(path.Join(parts...))
	text = classificationSeparators.ReplaceAllString(text, " ")
	for _, entry := range typeTerms {
		for _, term := range entry.Terms {
			if strings.Contains(text, term) {
				return entry.Type, "inferred"
			}
		}
	}
	return "other", "inferred"
}

func aiType(payload map[string]string) string {
	if explicit := strings.TrimSpace(payload["external_material_type"]); explicit != "" {
		if _, ok := TypeFolders[explicit]; ok {
			return explicit
		}
	}
	kind := strings.TrimSpace(payload["material_type"])
	if kind == "" {
		return ""
	}
	if result, ok := analysisTypes[kind]; ok {
		return result
	}
	return "other"
}

func sourceLabel(source string) *string {
	value := ""
	switch {
	case strings.Contains(fold(source), "/external/from-discord/"):
		value = "Discord"
	case strings.Contains(fold(source), "/external/inbox/"):
		value = "Browser upload"
	}
	if value == "" {
		return nil
	}
	return &value
}

// AnalysisType maps the shared derived vocabulary into external-library categories.
func AnalysisType(kind string) string { return analysisTypes[kind] }
