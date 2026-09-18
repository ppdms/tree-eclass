package knowledge

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/jobs"
	"tree-eclass/internal/infrastructure/storage"
	"tree-eclass/internal/infrastructure/storage/queries"
)

func publishArchive(ctx context.Context, tx pgx.Tx, parent queries.KnowledgeDocument, members []archiveMember) error {
	current := make([]string, 0, len(members))
	for _, member := range members {
		if err := publishMember(ctx, tx, parent, member); err != nil {
			return err
		}
		current = append(current, member.ID)
	}
	_, err := tx.Exec(
		ctx,
		`UPDATE knowledge.documents d SET is_current=0 WHERE d.is_current=1 AND d.id IN(SELECT child_document_id FROM knowledge.archive_members WHERE parent_document_id=$1) AND NOT(d.id=ANY($2::text[]))`,
		parent.ID,
		current,
	)
	return err
}
func publishMember(ctx context.Context, tx pgx.Tx, parent queries.KnowledgeDocument, m archiveMember) error {
	q := queries.New(tx)
	o := m.Object
	if err := q.QueueLock(ctx, "document:"+m.ID); err != nil {
		return err
	}
	if err := storage.RegisterObject(ctx, tx, o); err != nil {
		return err
	}
	if err := q.RegisterRevision(
		ctx,
		queries.RegisterRevisionParams{
			ID:          identity.Stable("rev", m.ID, o.SHA256),
			DocumentID:  m.ID,
			CourseID:    parent.CourseID,
			LogicalPath: m.Path,
			ObjectID:    o.SHA256,
		},
	); err != nil {
		return err
	}
	status, err := upsertArchiveDocument(ctx, tx, parent, m)
	if err != nil {
		return err
	}
	if err := upsertArchiveMemberRow(ctx, tx, parent, m); err != nil {
		return err
	}
	if status == "pending" {
		_, err = jobs.EnqueueTx(ctx, tx, "index", "index_document", map[string]string{"document_id": m.ID}, false)
	}
	return err
}

func upsertArchiveDocument(
	ctx context.Context,
	tx pgx.Tx,
	parent queries.KnowledgeDocument,
	m archiveMember,
) (string, error) {
	r, o := m.Record, m.Object
	var status string
	err := tx.QueryRow(ctx, `INSERT INTO knowledge.documents(id,course_id,course_name,course_short_name,source_path,normalized_path,display_name,source_hash,source_fingerprint,mime_type,document_kind,source_size_bytes,status,source_origin,content_hash_verified)
 VALUES($1,$2,$3,$4,$5,$5,$6,$7,$7,$8,$9,$10,'pending',$11,1)
 ON CONFLICT(id) DO UPDATE SET course_name=excluded.course_name,course_short_name=excluded.course_short_name,display_name=excluded.display_name,
 source_hash=excluded.source_hash,source_fingerprint=excluded.source_fingerprint,mime_type=excluded.mime_type,document_kind=excluded.document_kind,source_size_bytes=excluded.source_size_bytes,is_current=1,content_hash_verified=1,
 status=CASE WHEN knowledge.documents.source_hash=excluded.source_hash AND knowledge.documents.is_current=1 AND knowledge.documents.status='ready' THEN 'ready' ELSE 'pending' END
 WHERE knowledge.documents.course_id=excluded.course_id AND knowledge.documents.normalized_path=excluded.normalized_path AND knowledge.documents.source_origin=excluded.source_origin
 RETURNING status`,
		m.ID,
		parent.CourseID,
		parent.CourseName,
		parent.CourseShortName,
		m.Path,
		identity.Encode(m.Name),
		o.SHA256,
		o.MediaType,
		r.Kind,
		o.Bytes,
		parent.SourceOrigin,
	).
		Scan(&status)
	if err != nil {
		return "", err
	}
	// A newly admitted container revision explicitly restores this member version.
	if _, err = tx.Exec(ctx, `UPDATE app.document_revisions SET deleted_at=NULL WHERE document_id=$1 AND object_id=$2 AND course_id=$3 AND logical_path=$4`, m.ID, o.SHA256, parent.CourseID, m.Path); err != nil {
		return "", err
	}
	return status, nil
}

func upsertArchiveMemberRow(ctx context.Context, tx pgx.Tx, parent queries.KnowledgeDocument, m archiveMember) error {
	r, o := m.Record, m.Object
	chain, err := json.Marshal(r.MemberChain)
	if err != nil {
		return err
	}
	_, err = tx.Exec(
		ctx,
		`INSERT INTO knowledge.archive_members(child_document_id,parent_document_id,member_path,normalized_member_path,member_chain_json,depth,archive_format,parent_source_hash,parent_source_fingerprint,member_hash,crc32,compressed_size,expanded_size,member_kind,mime_type)
 VALUES($1,$2,$3,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
 ON CONFLICT(child_document_id) DO UPDATE SET member_path=excluded.member_path,normalized_member_path=excluded.normalized_member_path,member_chain_json=excluded.member_chain_json,depth=excluded.depth,archive_format=excluded.archive_format,parent_source_hash=excluded.parent_source_hash,parent_source_fingerprint=excluded.parent_source_fingerprint,member_hash=excluded.member_hash,crc32=excluded.crc32,compressed_size=excluded.compressed_size,expanded_size=excluded.expanded_size,member_kind=excluded.member_kind,mime_type=excluded.mime_type
 WHERE knowledge.archive_members.parent_document_id=excluded.parent_document_id`,
		m.ID,
		parent.ID,
		identity.Encode(r.MemberPath),
		string(chain),
		r.Depth,
		r.ArchiveFormat,
		parent.SourceHash,
		parent.SourceFingerprint,
		r.ContentHash,
		int64(r.CRC32),
		r.CompressedSize,
		r.ExpandedSize,
		r.Kind,
		o.MediaType,
	)
	return err
}
