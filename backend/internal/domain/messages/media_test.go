package messages

import (
	"fmt"
	"testing"

	"tree-eclass/internal/infrastructure/blob"
)

func TestAttachmentPublicationRequiresCompleteBytes(t *testing.T) {
	media := map[string]blob.Reference{"media/notes.txt": {SHA256: "immutable", Bytes: 33}}
	for _, size := range []int{0, 32, 33, 34} {
		m := stagedMessage{Attachments: fmt.Sprintf(`[{"url":"media/notes.txt","fileSizeBytes":%d}]`, size)}
		err := mapAttachments(&m, media)
		if (err == nil) != (size == 33) {
			t.Fatalf("size %d: %v", size, err)
		}
	}
}
