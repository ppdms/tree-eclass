package activity

import (
	"fmt"
	"regexp"
	"strings"

	"golang.org/x/text/cases"
	"golang.org/x/text/language"
)

var important = regexp.MustCompile(`(?i)exam|deadline|cancel|urgent|important|submission`)

func Groups(items []Item) []Group {
	groups := []Group{}
	byKey := map[string]int{}
	for _, item := range items {
		group, key := groupHeader(item)
		index, found := byKey[key]
		if !found {
			index = len(groups)
			byKey[key] = index
			group.ID = strings.ReplaceAll(key, " ", "-")
			groups = append(groups, group)
		}
		groups[index].Items = append(groups[index].Items, item)
	}
	return groups
}

func groupHeader(item Item) (Group, string) {
	g := Group{Type: item.Type, Importance: "informational", Link: "/announcements"}
	if item.Type == "change" {
		name := item.CourseName
		if item.ShortName != nil && *item.ShortName != "" {
			name = *item.ShortName
		}
		if name == "" {
			name = "Course files"
		}
		g.Title = "Course files changed in " + name
		if item.CourseID != nil {
			g.Link = fmt.Sprintf("/courses/%d/changes/%s", *item.CourseID, item.ChangeNo)
		}
		return g, "change:" + item.key()
	}
	g.Title = item.Title
	if g.Title == "" {
		g.Title = "Course announcement"
	}
	if important.MatchString(g.Title) {
		g.Importance = "important"
	}
	key := cases.Lower(language.Und).String(strings.Join(strings.Fields(g.Title), " "))
	return g, "announcement:" + key
}
