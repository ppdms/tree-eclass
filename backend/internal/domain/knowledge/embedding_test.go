package knowledge

import (
	"encoding/hex"
	"encoding/json"
	"math"
	"os"
	"reflect"
	"testing"
)

func TestEmbeddingFixture(t *testing.T) {
	var fixtures []struct{ Text, Packed string }
	data, err := os.ReadFile("testdata/embeddings.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		vector := Embed(fixture.Text)
		packed, err := hex.DecodeString(fixture.Packed)
		if err != nil {
			t.Fatal(err)
		}
		if hex.EncodeToString(Pack(vector)) != fixture.Packed {
			t.Errorf("embedding differs from the fixture for %q", fixture.Text)
		}
		if fixture.Text != "" && math.Abs(CosinePacked(vector, packed)-1) > 1e-6 {
			t.Error("packed cosine is not normalized")
		}
	}
}
func TestMetricFixture(t *testing.T) {
	var fixtures []struct {
		Texts   []string
		Metrics Metrics
	}
	data, err := os.ReadFile("testdata/metrics.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &fixtures); err != nil {
		t.Fatal(err)
	}
	for _, fixture := range fixtures {
		var counter metricAccumulator
		for _, text := range fixture.Texts {
			counter.Add(text)
		}
		if got := counter.Result(); !reflect.DeepEqual(got, fixture.Metrics) {
			t.Errorf("metrics differ from fixture: got %+v, expected %+v", got, fixture.Metrics)
		}
	}
}
func TestBoundedCandidatesKeepBestWithStableTies(t *testing.T) {
	var top candidates
	for i := 1000; i >= 0; i-- {
		top.admit(
			candidate{SearchResult: SearchResult{Score: float64(i % 10), DocumentID: "same"}, Ordinal: int64(i)},
			7,
		)
	}
	if len(top) != 7 {
		t.Fatalf("retained %d candidates", len(top))
	}
	for _, item := range top {
		if item.Score != 9 || item.Ordinal > 69 {
			t.Errorf("wrong candidate %+v", item)
		}
	}
}
