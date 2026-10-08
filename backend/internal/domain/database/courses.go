package database

// AppCourse is the neutral course row. Name and WebdavFolder stay encoded
// exactly as stored; callers decode via identity.Decode.
type AppCourse struct {
	ID           int64   `json:"id"`
	Name         string  `json:"name"`
	WebdavFolder string  `json:"webdav_folder"`
	SortOrder    *int64  `json:"sort_order"`
	Hidden       int64   `json:"hidden"`
	ShortName    *string `json:"short_name"`
}
