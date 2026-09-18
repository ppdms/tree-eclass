package eclass

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"golang.org/x/net/html"
)

type Exercise struct {
	ID                 string `json:"exercise_id"`
	Title              string `json:"title"`
	Link               string `json:"link"`
	Deadline           string `json:"deadline"`
	SubmissionStatus   string `json:"submission_status"`
	Grade              string `json:"grade"`
	WorkType           string `json:"work_type"`
	Description        string `json:"description"`
	StartDate          string `json:"start_date"`
	MaxGrade           string `json:"max_grade"`
	AssignmentFileName string `json:"assignment_file_name"`
	AssignmentFileURL  string `json:"assignment_file_url"`
	GradeComments      string `json:"grade_comments"`
	SubmissionDate     string `json:"submission_date"`
}

var numericID = regexp.MustCompile(`^[0-9]+$`)

func exerciseRow(row *html.Node, base *url.URL) *Exercise {
	cells := elements(row, "td")
	if len(cells) < 3 {
		return nil
	}
	a := first(cells[0], "a", "")
	if a == nil {
		return nil
	}
	u, err := base.Parse(attr(a, "href"))
	if err != nil || u.Host != base.Host || u.Scheme != base.Scheme || !numericID.MatchString(u.Query().Get("id")) {
		return nil
	}
	ex := &Exercise{
		ID:               u.Query().Get("id"),
		Title:            nodeText(a),
		Link:             u.String(),
		Deadline:         firstText(cells[1], false),
		SubmissionStatus: "pending",
		WorkType:         nodeText(first(cells[0], "small", "text-muted")),
	}
	if class(first(cells[2], "i", ""), "fa-check") {
		ex.SubmissionStatus = "submitted"
	}
	if len(cells) >= 4 {
		ex.Grade = nodeText(cells[3])
		if ex.Grade == "-" {
			ex.Grade = ""
		}
	}
	return ex
}

func ParseExercises(data []byte, base string, courseID int64) ([]Exercise, error) {
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	u, err := url.Parse(base)
	if err != nil {
		return nil, err
	}
	out := []Exercise{}
	for _, table := range elements(doc, "table") {
		if attr(table, "id") != fmt.Sprintf("assignment_table_INF%d", courseID) {
			continue
		}
		for _, row := range elements(first(table, "tbody", ""), "tr") {
			if ex := exerciseRow(row, u); ex != nil {
				out = append(out, *ex)
			}
		}
		return out, nil
	}
	// Empty courses may have an explicit no-assignments notice rather than a table.
	if strings.Contains(string(data), "Δεν υπάρχουν εργασίες") || disabledWorkModule(doc, u, courseID) {
		return out, nil
	}
	return nil, errors.New("exercise table is missing from the eClass response")
}

func disabledWorkModule(doc *html.Node, base *url.URL, courseID int64) bool {
	expected := fmt.Sprintf("/courses/INF%d", courseID)
	for _, table := range elements(doc, "table") {
		if attr(table, "id") != "portfolio_lessons" {
			continue
		}
		for _, link := range elements(table, "a") {
			target, err := base.Parse(attr(link, "href"))
			if err == nil && target.Scheme == base.Scheme && target.Host == base.Host &&
				strings.TrimRight(target.Path, "/") == expected {
				return true
			}
		}
	}
	return false
}

func cardFields(card *html.Node) map[string]*html.Node {
	fields := map[string]*html.Node{}
	for _, li := range elements(card, "li") {
		if !class(li, "list-group-item") {
			continue
		}
		label := first(li, "div", "title-default")
		value := first(li, "div", "title-default-line-height")
		if label != nil && value != nil {
			fields[strings.TrimSuffix(nodeText(label), ":")] = value
		}
	}
	return fields
}

func workDetails(ex *Exercise, fields map[string]*html.Node, base *url.URL) {
	ex.Description = innerHTML(fields["Περιγραφή"])
	ex.StartDate = firstText(fields["Ημερομηνία Έναρξης"], true)
	ex.MaxGrade = nodeText(fields["Μέγιστη βαθμολογία"])
	if a := first(fields["Αρχείο"], "a", ""); a != nil {
		ex.AssignmentFileName = attr(a, "title")
		if ex.AssignmentFileName == "" {
			ex.AssignmentFileName = nodeText(a)
		}
		if u, err := base.Parse(attr(a, "href")); err == nil && u.User == nil &&
			(u.Scheme == "https" || u.Scheme == "http") {
			ex.AssignmentFileURL = u.String()
		}
	}
}

func ParseExerciseDetail(data []byte, base string, ex *Exercise) error {
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return err
	}
	u, err := url.Parse(base)
	if err != nil {
		return err
	}
	found := false
	for _, card := range elements(doc, "div") {
		if !class(card, "panelCard") {
			continue
		}
		section := nodeText(first(first(card, "div", "card-header"), "h3", ""))
		fields := cardFields(first(card, "div", "card-body"))
		if strings.Contains(section, "Στοιχεία εργασίας") {
			workDetails(ex, fields, u)
			found = true
		}
		if strings.Contains(section, "Στοιχεία υποβολής") {
			if grade := nodeText(fields["Βαθμός"]); grade != "" && grade != "-" {
				ex.Grade = grade
			}
			ex.GradeComments = nodeText(fields["Σχόλια βαθμολογητή"])
			ex.SubmissionDate = nodeText(fields["Ημ/νία αποστολής"])
		}
	}
	if !found {
		return errors.New("exercise details are missing from the eClass response")
	}
	return nil
}

func (c *Client) Exercises(ctx context.Context, courseID int64) ([]Exercise, error) {
	data, err := c.Page(ctx, fmt.Sprintf("/modules/work/index.php?course=INF%d", courseID))
	if err != nil {
		return nil, err
	}
	list, err := ParseExercises(data, c.base.String(), courseID)
	if err != nil {
		return nil, err
	}
	for i := range list {
		data, err = c.Page(ctx, list[i].Link)
		if err != nil {
			return nil, err
		}
		if err = ParseExerciseDetail(data, c.base.String(), &list[i]); err != nil {
			return nil, err
		}
	}
	return list, nil
}
