UPDATE app.objects SET version_id=sha256 WHERE version_id IS DISTINCT FROM sha256;
