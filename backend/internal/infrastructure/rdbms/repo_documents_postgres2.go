package rdbms

import (
	"context"
	"strconv"

	"tree-eclass/internal/domain/database"
)

func (d postgresDocuments) EmbeddedCandidates(ctx context.Context, filter database.DocumentFilter,
	model string, dimensions int) (database.Iterator[database.EmbeddedCandidate], error) {
	scope, args := searchScopePG(filter, 1, nil)
	args = append(args, model, dimensions)
	scope += ` AND e.model=$` + strconv.Itoa(len(args)-1) + ` AND e.dimensions=$` + strconv.Itoa(len(args))
	rows, err := d.db.Query(ctx, `SELECT `+searchSelectPG+`,e.vector`+
		` FROM knowledge.chunk_embeddings e JOIN knowledge.chunks c ON c.id=e.chunk_id`+
		` JOIN knowledge.documents d ON d.id=c.document_id WHERE `+scope+
		` ORDER BY d.id,c.ordinal`, args...)
	if err != nil {
		return nil, err
	}
	return typedIterator(rows, func(rows nativeRows) (database.EmbeddedCandidate, error) {
		return scanSearchCandidate(rows, true)
	}), nil
}

func (d postgresDocuments) ReadChunks(ctx context.Context, document string, ordinals []int64,
	all bool) ([]database.DocumentChunk, error) {
	args := []any{document}
	query := `SELECT c.id,c.ordinal,c.locator_type,c.locator_start,c.locator_end,c.heading,` +
		`c.metadata_json,c.text FROM knowledge.chunks c WHERE c.document_id=$1`
	if !all {
		query += ` AND c.ordinal IN` + pgPlaceholders(2, len(ordinals))
		for _, ordinal := range ordinals {
			args = append(args, ordinal)
		}
	}
	query += ` ORDER BY c.ordinal`
	rows, err := d.db.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.DocumentChunk{}
	for rows.Next() {
		var item database.DocumentChunk
		if err := rows.Scan(&item.ID, &item.Ordinal, &item.LocatorType, &item.LocatorStart,
			&item.LocatorEnd, &item.Heading, &item.MetadataJSON, &item.Text); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (d postgresDocuments) ChunkLocators(ctx context.Context, document string) ([]database.ChunkLocator, error) {
	rows, err := d.db.Query(ctx, `SELECT ordinal,locator_type,coalesce(locator_start,'')`+
		` FROM knowledge.chunks WHERE document_id=$1 ORDER BY ordinal`, document)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.ChunkLocator{}
	for rows.Next() {
		var item database.ChunkLocator
		if err := rows.Scan(&item.Ordinal, &item.LocatorType, &item.LocatorStart); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (d postgresDocuments) GuideNavigation(ctx context.Context, course int64) (materials, headings []string,
	err error) {
	materials, headings = []string{}, []string{}
	rows, qerr := d.db.Query(ctx, `WITH selected AS (`+
		`SELECT d.id,d.display_name,d.normalized_path FROM knowledge.documents d`+
		` WHERE d.course_id=$1 AND `+documentAdmissionPG+` ORDER BY d.normalized_path,d.id LIMIT 100)`+
		` SELECT 'material',display_name,normalized_path FROM selected`+
		` UNION ALL SELECT 'heading',heading,heading FROM (`+
		`SELECT DISTINCT k.heading FROM knowledge.chunks k JOIN selected s ON s.id=k.document_id`+
		` WHERE k.heading IS NOT NULL AND k.heading<>'' ORDER BY k.heading LIMIT 100) h ORDER BY 1,3`, course)
	if qerr != nil {
		return materials, headings, qerr
	}
	defer rows.Close()
	for rows.Next() {
		var kind, value, order string
		if err := rows.Scan(&kind, &value, &order); err != nil {
			return materials, headings, err
		}
		if kind == "material" {
			materials = append(materials, value)
		} else {
			headings = append(headings, value)
		}
	}
	return materials, headings, rows.Err()
}

func (d postgresDocuments) ContentObject(ctx context.Context, course int64, document,
	revision string) (database.ContentObject, error) {
	var out database.ContentObject
	err := d.db.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type,d.display_name`+
		` FROM knowledge.documents d JOIN app.courses c ON c.id=d.course_id`+
		` JOIN app.document_revisions r ON r.document_id=d.id AND r.course_id=d.course_id`+
		` AND r.logical_path=d.normalized_path AND r.deleted_at IS NULL`+
		` JOIN app.objects o ON o.id=r.object_id`+
		` WHERE d.id=$1 AND d.course_id=$2 AND d.status='ready' AND `+documentAdmissionPG+
		` AND (c.hidden=0 OR EXISTS(SELECT 1 FROM app.course_exam_plans p`+
		` WHERE p.course_id=c.id AND p.enabled=1))`+
		` AND (($3='' AND o.sha256=d.source_hash) OR ($3<>'' AND r.id=$3))`+
		` ORDER BY r.created_at DESC LIMIT 1`, document, course, revision).
		Scan(&out.Bucket, &out.Key, &out.VersionID, &out.SHA256, &out.Bytes, &out.MediaType, &out.Name)
	return out, err
}

func (d postgresDocuments) LogicalContentObject(ctx context.Context, logical string) (database.ContentObject,
	string, error) {
	var out database.ContentObject
	var path string
	err := d.db.QueryRow(ctx, `SELECT o.bucket,o.key,o.version_id,o.sha256,o.bytes,o.media_type,r.logical_path`+
		` FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id`+
		` JOIN app.courses c ON c.id=r.course_id WHERE r.deleted_at IS NULL AND c.hidden=0 AND (`+
		`(r.logical_path=$1 AND EXISTS(SELECT 1 FROM knowledge.documents d WHERE d.id=r.document_id`+
		` AND d.course_id=r.course_id AND d.source_hash=o.sha256 AND `+documentAdmissionPG+`))`+
		` OR EXISTS(SELECT 1 FROM app.file_versions v WHERE v.revision_id=r.id AND v.version_webdav_path=$1))`+
		` ORDER BY r.created_at DESC LIMIT 1`, logical).
		Scan(&out.Bucket, &out.Key, &out.VersionID, &out.SHA256, &out.Bytes, &out.MediaType, &path)
	return out, path, err
}

func (d postgresDocuments) DocumentAnalysis(ctx context.Context, id string) (database.DocumentEnrichment, error) {
	var out database.DocumentEnrichment
	err := d.db.QueryRow(ctx, `SELECT e.status,e.source_hash,e.model,coalesce(e.requested_model,e.model),`+
		`e.analysis_version,CASE WHEN octet_length(e.payload_json)<=1048576 THEN e.payload_json END,`+
		`e.generated_at,d.document_kind,d.source_hash FROM knowledge.document_enrichments e`+
		` JOIN knowledge.documents d ON d.id=e.document_id JOIN app.courses c ON c.id=d.course_id`+
		` WHERE e.document_id=$1 AND d.is_current=1 AND d.status='ready' AND c.hidden=0`, id).
		Scan(&out.Status, &out.SourceHash, &out.Model, &out.Requested, &out.Version,
			&out.Payload, &out.GeneratedAt, &out.DocumentKind, &out.CurrentHash)
	return out, err
}

func (d postgresDocuments) PageEnrichments(ctx context.Context, document string, first, last int64,
	maxBytes int64) ([]database.PageEnrichment, error) {
	rows, err := d.db.Query(ctx, `SELECT page_number,status,model,generated_at,source_hash,`+
		`CASE WHEN octet_length(payload_json)<=$4 THEN payload_json END,analysis_version,requested_model`+
		` FROM knowledge.page_enrichments WHERE document_id=$1 AND page_number BETWEEN $2 AND $3`+
		` ORDER BY page_number`, document, first, last, maxBytes)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.PageEnrichment{}
	for rows.Next() {
		var item database.PageEnrichment
		if err := rows.Scan(&item.PageNumber, &item.Status, &item.Model, &item.GeneratedAt,
			&item.SourceHash, &item.Payload, &item.Version, &item.Requested); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (d postgresDocuments) PageAnalysis(ctx context.Context,
	params database.PageAnalysisParams) (database.PageEnrichment, error) {
	var out database.PageEnrichment
	out.PageNumber = params.Page
	err := d.db.QueryRow(ctx, `SELECT status,model,CASE WHEN octet_length(payload_json)<=$6 THEN payload_json END,`+
		`generated_at FROM knowledge.page_enrichments`+
		` WHERE document_id=$1 AND page_number=$2 AND source_hash=$3 AND analysis_version=$4 AND requested_model=$5`,
		params.Document, params.Page, params.Hash, params.Version, params.Model, params.MaxBytes).
		Scan(&out.Status, &out.Model, &out.Payload, &out.GeneratedAt)
	out.SourceHash, out.Version, out.Requested = params.Hash, params.Version, params.Model
	return out, err
}
func (d postgresDocuments) PageCoverage(ctx context.Context,
	params database.PageCoverageParams) ([]database.PageStatusCount, error) {
	rows, err := d.db.Query(ctx, `SELECT status,count(*),min(model) FROM knowledge.page_enrichments`+
		` WHERE document_id=$1 AND source_hash=$2 AND analysis_version=$3 AND requested_model=$4 GROUP BY status`,
		params.Document, params.Hash, params.Version, params.Model)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []database.PageStatusCount{}
	for rows.Next() {
		var item database.PageStatusCount
		if err := rows.Scan(&item.Status, &item.Count, &item.Model); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}

func (d postgresDocuments) RelatedMaterials(ctx context.Context, course int64,
	paths []string) ([]database.RelatedMaterial, error) {
	out := []database.RelatedMaterial{}
	if len(paths) == 0 {
		return out, nil
	}
	args := make([]any, 0, len(paths)+1)
	args = append(args, course)
	for _, path := range paths {
		args = append(args, path)
	}
	order := "CASE d.normalized_path"
	for i := range paths {
		order += ` WHEN $` + strconv.Itoa(i+2) + ` THEN ` + strconv.Itoa(i)
	}
	order += ` END`
	rows, err := d.db.Query(ctx, `SELECT d.source_path,d.display_name FROM knowledge.documents d`+
		` WHERE d.course_id=$1 AND d.normalized_path IN`+pgPlaceholders(2, len(paths))+
		` AND d.is_current=1 AND d.status='ready' ORDER BY `+order, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var item database.RelatedMaterial
		if err := rows.Scan(&item.Path, &item.Name); err != nil {
			return nil, err
		}
		out = append(out, item)
	}
	return out, rows.Err()
}
