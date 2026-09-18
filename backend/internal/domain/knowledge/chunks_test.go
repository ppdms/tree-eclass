package knowledge

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"

	"tree-eclass/internal/integrations/parser"
)

func TestChunkFixture(t *testing.T) {
	var fixture struct {
		Document, Hash string
		Unit           parser.Record
		Chunks         []Chunk
	}
	b, err := os.ReadFile("testdata/chunks.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	actual := Chunks(fixture.Document, fixture.Hash, fixture.Unit, 0)
	if !reflect.DeepEqual(actual, fixture.Chunks) {
		t.Fatal("chunk IDs, overlap, locators or Greek normalization differ from the established contract")
	}
}
