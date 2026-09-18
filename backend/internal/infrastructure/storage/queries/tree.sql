-- name: TreeNodes :many
SELECT id,parent_id,name,url,local_path FROM app.nodes WHERE course_id=$1 ORDER BY id;

-- name: TreeFiles :many
SELECT f.node_id,f.url,f.name,f.md5_hash,f.etag,f.last_updated,f.local_path,f.redirect_url
FROM app.files f JOIN app.nodes n ON n.id=f.node_id WHERE n.course_id=$1 ORDER BY f.id;
