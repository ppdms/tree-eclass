package knowledge

import (
	"context"
	"encoding/json"

	"tree-eclass/internal/domain/commands"
	"tree-eclass/internal/domain/database"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/objects"
)

func publishArchive(
	ctx context.Context, tx database.Tx, parent database.KnowledgeDocument, members []archiveMember,
) error {
	current := make([]string, 0, len(members))
	for _, member := range members {
		if err := publishMember(ctx, tx, parent, member); err != nil {
			return err
		}
		current = append(current, member.ID)
	}
	return tx.Indexing().RetireMissingArchiveMembers(ctx, parent.ID, current)
}
func publishMember(ctx context.Context, tx database.Tx, parent database.KnowledgeDocument, m archiveMember) error {
	o := m.Object
	if err := tx.Jobs().QueueLock(ctx, "document:"+m.ID); err != nil {
		return err
	}
	if err := objects.RegisterObject(ctx, tx, o); err != nil {
		return err
	}
	if err := tx.Objects().RegisterRevision(
		ctx,
		database.RegisterRevisionParams{
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
		_, err = commands.EnqueueTx(ctx, tx, "index", "index_document", map[string]string{"document_id": m.ID}, false)
	}
	return err
}

func upsertArchiveDocument(
	ctx context.Context,
	tx database.Tx,
	parent database.KnowledgeDocument,
	m archiveMember,
) (string, error) {
	r, o := m.Record, m.Object
	return tx.Indexing().UpsertArchiveDocument(ctx, database.ArchiveDocumentParams{
		ID:              m.ID,
		CourseID:        parent.CourseID,
		CourseName:      parent.CourseName,
		CourseShortName: parent.CourseShortName,
		Path:            m.Path,
		Name:            identity.Encode(m.Name),
		SHA:             o.SHA256,
		Media:           o.MediaType,
		Kind:            r.Kind,
		Bytes:           o.Bytes,
		SourceOrigin:    parent.SourceOrigin,
	})
}

func upsertArchiveMemberRow(
	ctx context.Context, tx database.Tx, parent database.KnowledgeDocument, m archiveMember,
) error {
	r, o := m.Record, m.Object
	chain, err := json.Marshal(r.MemberChain)
	if err != nil {
		return err
	}
	return tx.Indexing().UpsertArchiveMember(ctx, database.ArchiveMemberParams{
		ChildDocumentID:  m.ID,
		ParentDocumentID: parent.ID,
		Path:             identity.Encode(r.MemberPath),
		ChainJSON:        string(chain),
		Depth:            r.Depth,
		ArchiveFormat:    r.ArchiveFormat,
		ParentHash:       parent.SourceHash,
		ParentPrint:      parent.SourceFingerprint,
		MemberHash:       r.ContentHash,
		CRC32:            int64(r.CRC32),
		CompressedSize:   r.CompressedSize,
		ExpandedSize:     r.ExpandedSize,
		MemberKind:       r.Kind,
		Media:            o.MediaType,
	})
}
