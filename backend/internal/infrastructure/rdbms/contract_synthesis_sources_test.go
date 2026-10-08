package rdbms_test

import (
	"reflect"
	"testing"

	"tree-eclass/internal/domain/database"
)

func TestSynthesisCommunitySearchDoesNotMatchVectorPositions(t *testing.T) {
	for _, backend := range contractBackends(t) {
		t.Run(backend.name, func(t *testing.T) {
			store := openContractStore(t, backend.cfg)
			seedCommunityArchive(t, store)
			for _, scenario := range []struct {
				terms []string
				ids   []string
			}{
				{[]string{"exam"}, []string{"conv_recent", "conv_old"}},
				{[]string{"1"}, []string{}},
				{[]string{"EXAM"}, []string{"conv_recent", "conv_old"}},
			} {
				ids, err := store.Synthesis().SearchCommunityIDs(t.Context(), database.SynthesisCommunityParams{
					CourseID: 811, Terms: scenario.terms, Limit: 10,
				})
				if err != nil || !reflect.DeepEqual(ids, scenario.ids) {
					t.Fatalf("synthesis terms %v = %v, %v; want %v", scenario.terms, ids, err, scenario.ids)
				}
			}
		})
	}
}
