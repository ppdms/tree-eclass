-- name: ListCourses :many
SELECT id,name,webdav_folder,sort_order,hidden,short_name FROM app.courses
WHERE hidden=0 OR sqlc.arg(include_hidden)::boolean ORDER BY sort_order,id;

-- name: Course :one
SELECT id,name,webdav_folder,sort_order,hidden,short_name FROM app.courses WHERE id=$1;

-- name: AddCourse :exec
INSERT INTO app.courses(id,name,webdav_folder) VALUES($1,$2,$3);

-- name: RenameCourse :execrows
UPDATE app.courses SET name=$2 WHERE id=$1;

-- name: HideCourse :execrows
UPDATE app.courses SET hidden=$2 WHERE id=$1;

-- name: OrderCourse :exec
UPDATE app.courses SET sort_order=$2 WHERE id=$1;

-- name: CourseCoverage :many
SELECT course_id,payload_json FROM read_model.course_coverage ORDER BY course_id;

-- name: RecentMaterials :many
SELECT course_id,payload_json FROM read_model.recent_materials ORDER BY ordinal;

-- name: StudyLevels :many
SELECT course_id,file_path,level FROM app.file_study ORDER BY course_id,file_path;

-- name: SetStudyLevel :exec
INSERT INTO app.file_study(course_id,file_path,level,last_updated) VALUES($1,$2,$3,$4)
ON CONFLICT(course_id,file_path) DO UPDATE SET level=excluded.level,last_updated=excluded.last_updated;
