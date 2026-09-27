package inference

import (
	"encoding/base64"
	"errors"

	dinference "tree-eclass/internal/domain/inference"
)

// Image is the shared provider contract from domain/inference.
type Image = dinference.Image

func imageContent(m Message) (any, error) {
	if len(m.Images) == 0 {
		return m.Content, nil
	}
	text, ok := m.Content.(string)
	if !ok || len(m.Images) > 8 {
		return nil, errors.New("invalid multimodal message")
	}
	blocks := []map[string]any{{"type": "text", "text": text}}
	size := 0
	for _, image := range m.Images {
		size += len(image.Data)
		if (image.MIMEType != "image/jpeg" && image.MIMEType != "image/png") || size > 8*1024*1024 {
			return nil, errors.New("invalid or oversized inference image")
		}
		if _, err := base64.StdEncoding.DecodeString(image.Data); err != nil {
			return nil, errors.New("invalid inference image encoding")
		}
		blocks = append(
			blocks,
			map[string]any{
				"type":      "image_url",
				"image_url": map[string]string{"url": "data:" + image.MIMEType + ";base64," + image.Data},
			},
		)
	}
	return blocks, nil
}
