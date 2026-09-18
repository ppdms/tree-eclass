package identity

// EncodeJSON preserves NUL and literal private-use characters through PostgreSQL
// JSONB, using the same reversible text codec as relational text columns. Call
// DecodeJSON only at the corresponding encoded storage boundary.
func EncodeJSON(value any) any { return mapJSON(value, Encode) }
func DecodeJSON(value any) any { return mapJSON(value, Decode) }

func mapJSON(value any, text func(string) string) any {
	switch v := value.(type) {
	case string:
		return text(v)
	case map[string]any:
		result := make(map[string]any, len(v))
		for key, item := range v {
			result[text(key)] = mapJSON(item, text)
		}
		return result
	case []any:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = mapJSON(item, text)
		}
		return result
	case []map[string]any:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = mapJSON(item, text)
		}
		return result
	case []string:
		result := make([]any, len(v))
		for i, item := range v {
			result[i] = text(item)
		}
		return result
	default:
		return value
	}
}
