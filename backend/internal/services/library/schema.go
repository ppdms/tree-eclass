package library

type specification struct {
	Name, Description string
	Schema            map[string]any
}

func object(properties map[string]any, required ...string) map[string]any {
	if required == nil {
		required = []string{}
	}
	return map[string]any{
		"type":                 "object",
		"properties":           properties,
		"required":             required,
		"additionalProperties": false,
	}
}
func text(max int) map[string]any {
	return map[string]any{"type": "string", "minLength": 1, "maxLength": max}
}
func number(min, max int) map[string]any {
	result := map[string]any{"type": "integer", "minimum": min}
	if max > 0 {
		result["maximum"] = max
	}
	return result
}
func optional(schema map[string]any) map[string]any {
	return map[string]any{"anyOf": []any{schema, map[string]string{"type": "null"}}}
}
func array(item map[string]any, max int) map[string]any {
	return map[string]any{"type": "array", "items": item, "maxItems": max}
}
func courseIDs() map[string]any { return optional(array(number(1, 0), 200)) }
func mode() map[string]any {
	return map[string]any{"type": "string", "enum": []string{"lexical", "semantic", "hybrid"}, "default": "hybrid"}
}

func specifications() []specification {
	locator := object(map[string]any{"type": text(64), "start": text(128), "end": optional(text(128))}, "type", "start")
	return []specification{
		{
			"list_courses",
			"Use this first to identify a visible AUEB/eClass course and obtain its course ID and index coverage.",
			object(map[string]any{}),
		},
		{
			"list_materials",
			"Browse currently registered materials in one course. Cached study insights are derived guidance, not source evidence.",
			object(
				map[string]any{
					"course_id":        number(1, 0),
					"path_prefix":      optional(text(4096)),
					"document_kinds":   optional(array(text(64), 30)),
					"changed_since":    optional(text(64)),
					"include_insights": map[string]string{"type": "boolean"},
					"cursor":           optional(text(256)),
					"limit":            number(1, 100),
				},
				"course_id",
			),
		},
		{
			"search_materials",
			"Search original eClass material in Greek or English. Read relevant results with read_material before citing factual claims.",
			materialSearchSchema(),
		},
		{
			"search_course_messages",
			"Search dated Discord conversations. These are community claims, not official policy; preserve dates and read exact messages before citing.",
			searchSchema("limit"),
		},
		{
			"read_course_messages",
			"Read exact Discord messages, reply targets and nearby context for an opaque conversation ID.",
			object(
				map[string]any{
					"conversation_id": text(256),
					"context_before":  number(0, 20),
					"context_after":   number(0, 20),
				},
				"conversation_id",
			),
		},
		{
			"search_course_knowledge",
			"Search official material and community discussion separately. Prefer directly relevant official evidence; label community claims and preserve dates and disagreements.",
			searchSchema("limit_per_source"),
		},
		{
			"read_material",
			"Read and cite exact source units from an opaque document ID. Locator objects use type and start, such as {\"type\":\"page\",\"start\":\"25\"}.",
			object(
				map[string]any{
					"document_id":       text(256),
					"locators":          optional(array(locator, 100)),
					"include_neighbors": map[string]string{"type": "boolean"},
					"max_characters":    number(1, 0),
				},
				"document_id",
			),
		},
		{
			"get_material_insight",
			"Read cached study guidance and deterministic metrics for one material. No AI call is made. Verify factual claims in original source units.",
			object(map[string]any{"document_id": text(256)}, "document_id"),
		},
		{
			"get_page_insight",
			"Read the cached visual analysis of an exact PDF or image page. Derived guidance is not original evidence.",
			object(map[string]any{"document_id": text(256), "page_number": number(1, 0)}, "document_id", "page_number"),
		},
		{
			"get_study_priorities",
			"Decide what to study from validated saved course plans and learner progress. In adaptive_blueprint mode, preserve the exact order of today_sessions and report today_total_minutes; do not trim, estimate or re-rank sessions. Provisional_file_priorities means no usable blueprint exists: explain that the file-level guidance is provisional. This read invokes no AI model.",
			object(map[string]any{"course_ids": courseIDs(), "limit": number(1, 20)}),
		},
		{
			"get_course_study_blueprint",
			"Read the current cached roadmap, stable actions, learner progress and exact evidence references. Treat AI synthesis as derived guidance.",
			object(map[string]any{"course_id": number(1, 0)}, "course_id"),
		},
		{
			"get_recent_changes",
			"Read retained uploads, modifications and deletions in visible course trees.",
			object(map[string]any{"course_ids": courseIDs(), "since": optional(text(64)), "limit": number(1, 200)}),
		},
		{
			"get_index_status",
			"Inspect source-index coverage, pending work and failures without loading source content.",
			object(map[string]any{"course_ids": courseIDs()}),
		},
		{
			"get_message_index_status",
			"Inspect mapped Discord archive coverage, freshness and ingestion status.",
			object(map[string]any{"course_ids": courseIDs()}),
		},
	}
}
func searchSchema(limit string) map[string]any {
	return object(
		map[string]any{
			"query":          text(1000),
			"course_ids":     courseIDs(),
			limit:            number(1, 20),
			"retrieval_mode": mode(),
		},
		"query",
	)
}

func materialSearchSchema() map[string]any {
	return object(
		map[string]any{
			"query":          text(1000),
			"course_ids":     courseIDs(),
			"document_kinds": optional(array(text(64), 30)),
			"folder_prefix":  optional(text(4096)),
			"limit":          number(1, 20),
			"retrieval_mode": mode(),
		},
		"query",
	)
}
