package inference

import "tree-eclass/internal/domain/settings"

// Course and practice synthesis use only explicitly requested models. Document
// lanes additionally honor the configured per-provider fallback order.
func AnalysisCandidates(a settings.AI, keys map[string]string, lane string) []Candidate {
	primary, provider, fallbacks := a.Model, a.Provider, a.DocumentFallbacks
	if lane == "page" {
		fallbacks = a.PageFallbacks
	}
	if lane == "course" {
		primary, fallbacks = a.CourseModel, a.CourseFallbacks
		provider = settings.ProviderForModel(primary)
	}
	if lane == "practice" {
		primary, fallbacks = a.PracticeModel, a.PracticeFallbacks
		provider = settings.ProviderForModel(primary)
	}
	models := map[string]string{
		"synthetic":   a.SyntheticModel,
		"ollama":      a.OllamaModel,
		"huggingface": a.HuggingFaceModel,
		"alibaba":     a.AlibabaModel,
		"zai":         a.ZAIModel,
	}
	if lane != "course" && lane != "practice" && settings.ProviderForModel(primary) != provider {
		primary = models[provider]
	}
	pairs := [][2]string{{provider, primary}}
	if lane != "course" && lane != "practice" {
		for _, name := range a.Order {
			if name != provider {
				pairs = append(pairs, [2]string{name, models[name]})
			}
		}
	}
	for _, model := range fallbacks {
		pairs = append(pairs, [2]string{settings.ProviderForModel(model), model})
	}
	result := []Candidate{}
	seen := map[[2]string]bool{}
	think := false
	for _, pair := range pairs {
		name, model := pair[0], pair[1]
		key := settings.ProviderKey(keys, name)
		if seen[pair] || model == "" || key == "" || !a.Enabled(name) || endpoints[name] == "" {
			continue
		}
		seen[pair] = true
		c := candidate(name, model, key, &think)
		c.ThinkingLevel = "medium"
		if lane == "page" {
			c.ThinkingLevel = "none"
		}
		if lane == "course" {
			c.ThinkingLevel = "high"
		}
		result = append(result, c)
	}
	return result
}
