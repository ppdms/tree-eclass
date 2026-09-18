package inference

import "errors"

// Analysis retains the existing task-level policy: none for pages, medium for
// file guides and practice, high for course synthesis. Chat uses its own toggle.
func analysisThinking(c Candidate, p map[string]any) error {
	level := c.ThinkingLevel
	if level == "" {
		return nil
	}
	if level != "none" && level != "medium" && level != "high" {
		return errors.New("invalid analysis thinking level")
	}
	switch c.Provider {
	case "synthetic", "huggingface":
		p["reasoning_effort"] = level
	case "ollama":
		if level == "none" {
			p["think"] = false
		} else {
			p["think"] = level
		}
	case "alibaba":
		p["enable_thinking"] = level != "none"
		delete(p, "reasoning_effort")
		if level != "none" {
			effort := map[string]string{"medium": "high", "high": "max"}[level]
			if c.Model == "qwen3.8-flash" {
				effort = map[string]string{"medium": "medium", "high": "xhigh"}[level]
			}
			p["reasoning_effort"] = effort
		}
		if c.Model == "deepseek-v4-flash-0731" {
			delete(p, "response_format")
		}
	case "zai":
		mode := "disabled"
		if level != "none" {
			mode = "enabled"
			p["reasoning_effort"] = map[string]string{"medium": "high", "high": "max"}[level]
		}
		p["thinking"] = map[string]string{"type": mode}
	}
	return nil
}
