package settings

// These versions identify the persisted analysis contracts, independently of
// whichever provider or model produced them.
const DocumentAnalysisVersion = "6"
const PageAnalysisVersion = "2"
const PageSynthesisVersion = DocumentAnalysisVersion + ".pages." + PageAnalysisVersion
const CourseAnalysisVersion = "2"
const PracticeAnalysisVersion = "1"
