package knowledge

// CurrentSourcePredicate uses the fixed SQL alias d. Source reads and analysis
// share this boundary: immutable catalog membership and every archive ancestor
// must agree with the current content identity. It does not require extraction
// completion, so pending/unsupported registered files remain browsable.
const CurrentSourcePredicate = `d.is_current=1 AND d.source_origin IN('eclass','external')
 AND EXISTS(SELECT 1 FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id
 WHERE r.document_id=d.id AND r.course_id=d.course_id AND r.logical_path=d.normalized_path AND r.deleted_at IS NULL AND o.sha256=d.source_hash)
 AND NOT EXISTS(WITH RECURSIVE source_ancestors AS(
 SELECT d.id current,','||d.id||',' trail,0 depth
 UNION ALL SELECT m.parent_document_id,a.trail||m.parent_document_id||',',a.depth+1 FROM source_ancestors a JOIN knowledge.archive_members m ON m.child_document_id=a.current WHERE instr(a.trail,','||m.parent_document_id||',')=0 AND a.depth<8)
 SELECT 1 FROM source_ancestors a JOIN knowledge.archive_members m ON m.child_document_id=a.current
 LEFT JOIN knowledge.documents child ON child.id=m.child_document_id LEFT JOIN knowledge.documents p ON p.id=m.parent_document_id
 WHERE p.id IS NULL OR p.course_id<>d.course_id OR p.is_current<>1 OR p.status<>'ready' OR p.content_hash_verified<>1 OR p.source_origin NOT IN('eclass','external')
 OR m.parent_source_hash<>p.source_hash OR m.member_hash<>child.source_hash OR instr(a.trail,','||m.parent_document_id||',')>0 OR a.depth>=8
 OR NOT EXISTS(SELECT 1 FROM app.document_revisions r JOIN app.objects o ON o.id=r.object_id WHERE r.document_id=p.id AND r.course_id=p.course_id AND r.logical_path=p.normalized_path AND r.deleted_at IS NULL AND o.sha256=p.source_hash))`
