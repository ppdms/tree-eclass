package server

import (
	"errors"
	"net/http"
	"path"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/domain/knowledge"
	"tree-eclass/internal/infrastructure/blob"
	"tree-eclass/internal/infrastructure/storage/queries"
)

func (s *Server) fileRoutes() {
	s.mux.HandleFunc("GET /api/discord/media/{object_id}", s.discordMedia)
	s.mux.HandleFunc("GET /api/study/document/{document_id}/content", s.documentContent)
	s.mux.HandleFunc("GET /api/files/{file_id}/content", s.fileContent)
	s.mux.HandleFunc("GET /files/{path...}", s.logicalContent)
}
func (s *Server) documentContent(w http.ResponseWriter, r *http.Request) {
	course, err := strconv.ParseInt(r.URL.Query().Get("course_id"), 10, 64)
	if err != nil || course < 1 {
		writeFailure(w, http.StatusUnprocessableEntity, "Course ID is required")
		return
	}
	content, err := (knowledge.Reader{Pool: s.db.Pool}).Content(
		r.Context(),
		course,
		r.PathValue("document_id"),
		r.URL.Query().Get("revision_id"),
	)
	if err != nil {
		s.fileError(w, err)
		return
	}
	w.Header().Set("X-Course-Id", strconv.FormatInt(course, 10))
	s.blobs.Serve(w, r, content.Object, content.Name)
}
func (s *Server) fileContent(w http.ResponseWriter, r *http.Request) {
	row, err := queries.New(s.db.Pool).FileObject(r.Context(), r.PathValue("file_id"))
	if err != nil {
		s.fileError(w, err)
		return
	}
	s.blobs.Serve(
		w,
		r,
		blob.Reference{
			Bucket:    row.Bucket,
			Key:       row.Key,
			VersionID: row.VersionID,
			SHA256:    row.Sha256,
			Bytes:     row.Bytes,
			MediaType: row.MediaType,
		},
		path.Base(row.LogicalPath),
	)
}
func (s *Server) logicalContent(w http.ResponseWriter, r *http.Request) {
	logical := identity.Path(r.PathValue("path"))
	if strings.HasPrefix(logical, "/_diffs/") {
		object, err := s.pdfDifferences().Content(r.Context(), logical)
		if err != nil {
			s.fileError(w, err)
			return
		}
		s.blobs.Serve(w, r, object, "difference.pdf")
		return
	}
	content, err := (knowledge.Reader{Pool: s.db.Pool}).LogicalContent(r.Context(), identity.Path(r.PathValue("path")))
	if err != nil {
		s.fileError(w, err)
		return
	}
	s.blobs.Serve(w, r, content.Object, content.Name)
}
func (s *Server) fileError(w http.ResponseWriter, err error) {
	if errors.Is(err, pgx.ErrNoRows) {
		writeFailure(w, http.StatusNotFound, "Document not found")
		return
	}
	s.internal(w, err)
}
