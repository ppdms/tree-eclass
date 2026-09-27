-- name: ActivityPage :many
WITH events AS (
    SELECT r.timestamp AS stamp,r.id,'change' AS kind,r.course_id
      FROM app.change_records r JOIN app.courses c ON c.id=r.course_id WHERE c.hidden=0
    UNION ALL
    SELECT a.pub_date,a.id,'announcement',a.course_id
      FROM app.announcements a JOIN app.courses c ON c.id=a.course_id WHERE c.hidden=0
    UNION ALL SELECT pub_date,id,'global',NULL::bigint FROM app.global_announcements
), page AS (SELECT * FROM events ORDER BY stamp DESC NULLS LAST,kind,id DESC LIMIT $1 OFFSET $2)
SELECT (jsonb_build_object(
    'type',CASE WHEN p.kind='change' THEN 'change' ELSE 'announcement' END,
    'id',CASE WHEN p.kind='global' THEN to_jsonb('global_' || p.id) ELSE to_jsonb(p.id) END,
    'timestamp',p.stamp,'sort_key',coalesce(p.stamp,''),'course_id',p.course_id,
    'course_name',coalesce(c.name,initcap(replace(coalesce(g.feed_key,'Global'),'_',' '))),
    'course_short_name',c.short_name
) || CASE WHEN p.kind='change' THEN jsonb_build_object(
    'change_no',r.change_no,'message',r.message,'changes',coalesce((
        SELECT jsonb_agg(jsonb_build_object('change_type',i.change_type,'file_path',i.file_path,
            'display_name',i.display_name,'redirect_url',i.redirect_url,'diff_webdav_path',i.diff_webdav_path)
            ORDER BY i.id) FROM app.change_record_items i WHERE i.change_record_id=r.id),'[]'))
    ELSE jsonb_build_object('title',coalesce(a.title,g.title),'link',coalesce(a.link,g.link),
                           'description',coalesce(a.description,g.description)) END)::jsonb AS payload
FROM page p LEFT JOIN app.courses c ON c.id=p.course_id
LEFT JOIN app.change_records r ON p.kind='change' AND r.id=p.id
LEFT JOIN app.announcements a ON p.kind='announcement' AND a.id=p.id
LEFT JOIN app.global_announcements g ON p.kind='global' AND g.id=p.id
ORDER BY p.stamp DESC NULLS LAST,p.kind,p.id DESC;
