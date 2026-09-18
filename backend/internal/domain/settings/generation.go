package settings

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// Pages are part of the native analysis pipeline for visual documents. Changes
// to this contract must change the generation, including parser/prompt versions.
func DocumentVersion(kind string) string {
	if kind == "pdf" || kind == "image" {
		return PageSynthesisVersion
	}
	return DocumentAnalysisVersion
}

func (a AI) AnalysisGeneration() string {
	data, _ := json.Marshal(struct {
		Model, CourseModel, PracticeModel                                              string
		DocumentVersion, PageVersion, SynthesisVersion, CourseVersion, PracticeVersion string
		Documents, Courses, Practice                                                   bool
	}{a.Model, a.CourseModel, a.PracticeModel, DocumentAnalysisVersion, PageAnalysisVersion, PageSynthesisVersion,
		CourseAnalysisVersion, PracticeAnalysisVersion, a.EnrichmentEnabled, a.CourseEnabled, a.PracticeEnabled})
	hash := sha256.Sum256(data)
	return hex.EncodeToString(hash[:])
}
