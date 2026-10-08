package rdbms

import (
	"context"
	"strconv"

	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
)

type postgresSynthesis struct{ db nativeDBTX }

type sqliteSynthesis struct{ db nativeDBTX }

func synthesisTable(lane database.SynthesisLane) string {
	if lane == database.SynthesisPractice {
		return "practice_question_sets"
	}
	return "course_blueprints"
}

func synthesisTablePG(lane database.SynthesisLane) string {
	return "knowledge." + synthesisTable(lane)
}

// synthesisDuePG matches rows whose stored TEXT availability has passed. Stored
// stamps are RFC3339Nano UTC text or legacy space-separated UTC text; both
// cast to timestamptz for the due comparison.
func synthesisDuePG(column string) string {
	return column + "::timestamptz<=clock_timestamp()"
}

// synthesisJSONText reads a top-level JSON string field from payload_json.
// Guard the cast with PostgreSQL's native JSON validation; SQLite uses
// its json_valid/json_extract pair for the same malformed-payload boundary.
func synthesisJSONTextPG(field string) string {
	return `(CASE WHEN pg_input_is_valid(payload_json,'jsonb')` +
		` THEN payload_json::jsonb->>'` + field + `' ELSE '' END)`
}

// synthesisReadySourcePG filters enrichment rows whose insight payload is
// usable: current requested model and generation, object-shaped payload under
// 1 MiB with a non-empty summary and no mismatch alignment.
func synthesisReadySourcePG(from int) string {
	// $from=requested model, $from+1=document version, $from+2=page version.
	return `d.status='ready' AND d.content_hash_verified=1` +
		` AND e.status='ready' AND e.source_hash=d.source_hash` +
		` AND coalesce(e.requested_model,e.model)=$` + strconv.Itoa(from) +
		` AND e.analysis_version=CASE WHEN d.document_kind IN('pdf','image')` +
		` THEN $` + strconv.Itoa(from+2) + ` ELSE $` + strconv.Itoa(from+1) + ` END` +
		` AND substr(ltrim(e.payload_json),1,1)='{'` +
		` AND octet_length(e.payload_json)<=1048576` +
		` AND coalesce(` + synthesisJSONTextPG("summary") + `,'')<>''` +
		` AND coalesce(` + synthesisJSONTextPG("course_alignment") + `,'')<>'mismatch'`
}

func synthesisReadyOrderPG() string {
	return `CASE coalesce(` + synthesisJSONTextPG("importance") + `,'')` +
		` WHEN 'essential' THEN 0 WHEN 'useful' THEN 1 ELSE 2 END,d.id`
}

// Match decoded source text, never backend-specific full-text index serialization.
func synthesisCommunityIDs(ctx context.Context, db nativeDBTX, query string,
	params database.SynthesisCommunityParams) ([]string, error) {
	out := []string{}
	if len(params.Terms) == 0 || params.Limit <= 0 {
		return out, nil
	}
	terms := make([]string, len(params.Terms))
	for i, term := range params.Terms {
		terms[i] = identity.Search(identity.Decode(term))
	}
	rows, err := db.Query(ctx, query, params.CourseID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id, text, channel string
		if err := rows.Scan(&id, &text, &channel); err != nil {
			return nil, err
		}
		if matchesLexicalText(text+" "+channel, terms) {
			out = append(out, id)
			if int64(len(out)) == params.Limit {
				return out, nil
			}
		}
	}
	return out, rows.Err()
}
