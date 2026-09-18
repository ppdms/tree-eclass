package activity

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
)

func TestActivityGroupsFixture(t *testing.T) {
	// Expected groups are checked against an isolated JSON fixture without
	// opening a database.
	data, err := os.ReadFile("testdata/groups.json")
	if err != nil {
		t.Fatal(err)
	}
	type header struct {
		ID, Type, Title, Importance, Link string
		Items                             []json.RawMessage
	}
	var fixture struct {
		Items  []Item
		Groups []header
	}
	if err = json.Unmarshal(data, &fixture); err != nil {
		t.Fatal(err)
	}
	got := []header{}
	for _, group := range Groups(fixture.Items) {
		item := header{
			ID:         group.ID,
			Type:       group.Type,
			Title:      group.Title,
			Importance: group.Importance,
			Link:       group.Link,
		}
		for _, child := range group.Items {
			item.Items = append(item.Items, child.ID)
		}
		got = append(got, item)
	}
	if !reflect.DeepEqual(got, fixture.Groups) {
		t.Fatalf("group identity or presentation changed: %#v", got)
	}
}
