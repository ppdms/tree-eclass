package server

import (
	"errors"
	"io"
	"log/slog"
	"mime/multipart"
	"net/http"

	"github.com/jackc/pgx/v5"
	"tree-eclass/internal/domain/materials"
	"tree-eclass/internal/domain/settings"
	"tree-eclass/internal/infrastructure/blob"
)

func (s *Server) materialRoutes() {
	s.mux.HandleFunc("GET /api/v1/courses/{course_id}/materials", s.listMaterials)
	s.mux.HandleFunc("POST /api/v1/courses/{course_id}/materials", s.uploadMaterial)
	s.mux.HandleFunc("PATCH /api/v1/courses/{course_id}/materials/{opaque_id}", s.materialType)
}
func (s *Server) listMaterials(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	rows, ok := s.materialList(w, r, id, "")
	if !ok {
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"materials": rows})
}
func (s *Server) uploadMaterial(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	upload, header, ok := prepareMaterialUpload(w, r, id)
	if !ok {
		return
	}
	defer r.MultipartForm.RemoveAll()
	defer func() {
		if closer, ok := upload.Body.(io.Closer); ok {
			_ = closer.Close()
		}
	}()
	service := materials.Service{Pool: s.db.Pool, Objects: s.blobs, Temp: s.config.Temp}
	result, err := service.Upload(r.Context(), upload)
	if err != nil {
		if errors.Is(err, materials.ErrDuplicate) {
			writeFailure(w, http.StatusConflict, err.Error())
			return
		}
		if errors.Is(err, pgx.ErrNoRows) {
			writeFailure(w, http.StatusNotFound, "Course not found")
			return
		}
		s.internal(w, err)
		return
	}
	if s.config.ExternalWorkers {
		if mirrorErr := s.syncService().MirrorExternal(r.Context(), id); mirrorErr != nil {
			slog.Error("external material mirror failed", "course_id", id, "error", mirrorErr)
		}
	}
	writeMaterialUpload(w, id, header, upload.Type, result)
}

func prepareMaterialUpload(
	w http.ResponseWriter,
	r *http.Request,
	id int64,
) (materials.Upload, *multipart.FileHeader, bool) {
	r.Body = http.MaxBytesReader(w, r.Body, blob.MaxSourceBytes+1024*1024)
	// Multipart's bounded in-memory threshold spills larger files to disk.
	if err := r.ParseMultipartForm(64 * 1024); err != nil {
		writeFailure(w, http.StatusRequestEntityTooLarge, "Upload is invalid or exceeds 50 MiB")
		return materials.Upload{}, nil, false
	}
	file, header, err := r.FormFile("file")
	if err != nil {
		writeFailure(w, http.StatusUnprocessableEntity, "Choose a file")
		return materials.Upload{}, nil, false
	}
	if _, err = materials.Filename(header.Filename); err != nil {
		file.Close()
		writeFailure(w, http.StatusUnprocessableEntity, err.Error())
		return materials.Upload{}, nil, false
	}
	if kind := r.FormValue("material_type"); kind != "" {
		if _, valid := materials.TypeFolders[kind]; !valid {
			file.Close()
			writeFailure(w, http.StatusUnprocessableEntity, "Choose a valid document type")
			return materials.Upload{}, nil, false
		}
	}
	if header.Size == 0 {
		file.Close()
		writeFailure(w, http.StatusUnprocessableEntity, "The selected file is empty")
		return materials.Upload{}, nil, false
	}
	if header.Size > blob.MaxSourceBytes {
		file.Close()
		writeFailure(w, http.StatusRequestEntityTooLarge, "Files must be no larger than 50 MiB")
		return materials.Upload{}, nil, false
	}
	kind := materials.Kind(header.Filename, header.Header.Get("Content-Type"))
	if kind == "" {
		file.Close()
		writeFailure(w, http.StatusUnsupportedMediaType, "This file type is not supported")
		return materials.Upload{}, nil, false
	}
	return materials.Upload{
		CourseID: id, Name: header.Filename, Type: r.FormValue("material_type"),
		MediaType: header.Header.Get("Content-Type"), Body: file,
	}, header, true
}

func writeMaterialUpload(
	w http.ResponseWriter,
	id int64,
	header *multipart.FileHeader,
	materialType string,
	result materials.Result,
) {
	classification := "pending"
	if materialType != "" {
		classification = "manual"
	} else {
		materialType = "other"
	}
	writeJSON(
		w,
		http.StatusAccepted,
		map[string]any{"status": "uploaded", "command_id": result.CommandID, "message": "Uploaded. Indexing is queued.",
			"material": map[string]any{
				"id":                    result.DocumentID,
				"document_id":           result.DocumentID,
				"revision_id":           result.RevisionID,
				"course_id":             id,
				"display_name":          header.Filename,
				"source_path":           result.Path,
				"source_origin":         "external",
				"material_type":         materialType,
				"classification_source": classification,
				"source_label":          "Browser upload",
				"document_kind":         result.Kind,
				"status":                "pending",
				"source_size_bytes":     result.Object.Bytes,
				"page_count":            0,
				"renderable":            false,
				"open_url":              nil,
				"download_url":          nil,
			}},
	)
}
func (s *Server) materialType(w http.ResponseWriter, r *http.Request) {
	id, ok := pathID(w, r, "course_id")
	if !ok {
		return
	}
	var body struct {
		Type string `json:"material_type"`
	}
	if !bodyJSON(w, r, &body) {
		return
	}
	if _, ok = materials.TypeFolders[body.Type]; !ok {
		writeFailure(w, http.StatusUnprocessableEntity, "Choose a valid document type")
		return
	}
	service := materials.Service{Pool: s.db.Pool}
	if err := service.UpdateType(r.Context(), id, r.PathValue("opaque_id"), body.Type); err != nil {
		s.fileError(w, err)
		return
	}
	rows, ok := s.materialList(w, r, id, r.PathValue("opaque_id"))
	if !ok {
		return
	}
	if len(rows) == 0 {
		writeFailure(w, http.StatusNotFound, "Material not found")
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"material": rows[0]})
}

func (s *Server) materialList(
	w http.ResponseWriter,
	r *http.Request,
	id int64,
	document string,
) ([]materials.Material, bool) {
	a, err := s.settingsService().AI(r.Context())
	if err != nil {
		s.internal(w, err)
		return nil, false
	}
	pending := a.EnrichmentEnabled && a.Enabled(a.Provider) &&
		settings.ProviderKey(s.config.ProviderKeys, a.Provider) != ""
	rows, err := (materials.Service{Pool: s.db.Pool}).List(r.Context(), id, document, pending)
	if err != nil {
		s.fileError(w, err)
		return nil, false
	}
	return rows, true
}
