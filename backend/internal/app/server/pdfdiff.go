package server

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"

	"tree-eclass/internal/integrations/pdfdiff"
)

func (s *Server) pdfDifferences() pdfdiff.Service {
	return pdfdiff.Service{
		Pool:    s.db.Pool,
		Objects: s.blobs,
		Temp:    s.config.Temp,
		Runner: pdfdiff.NativeRunner{
			Binary:    s.config.PDFDiff,
			SHA256:    s.config.PDFDiffSHA,
			Root:      s.config.ParserRoot,
			ToolsRoot: s.config.NativeToolsRoot,
			Registry:  filepath.Join(s.config.Temp, ".helpers"),
		},
	}
}
func (s *Server) runPDFDifference(ctx context.Context, payload []byte) error {
	var request struct {
		ID string `json:"difference_id"`
	}
	if err := json.Unmarshal(payload, &request); err != nil {
		return err
	}
	if len(request.ID) != 37 {
		return errors.New("invalid PDF difference identity")
	}
	return s.pdfDifferences().Process(ctx, request.ID)
}
