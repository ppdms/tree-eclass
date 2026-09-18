package messages

import (
	"fmt"
	"strings"
	"time"

	"tree-eclass/internal/domain/identity"
)

var policyTerms = []string{
	"grading",
	"grade",
	"assessment",
	"exam",
	"deadline",
	"instructor",
	"laboratory",
	"βαθμολογια",
	"βαθμος",
	"αξιολογηση",
	"εξεταση",
	"εργαστηριο",
	"προοδος",
	"υποχρεωτικη",
	"διδασκων",
	"προθεσμια",
	"ισχυει",
	"φετος",
}

func policyQuery(query string) bool {
	query = identity.Search(query)
	for _, term := range policyTerms {
		if strings.Contains(query, term) {
			return true
		}
	}
	return false
}
func reliableChannel(name string) bool {
	return strings.Contains(identity.Search(name), "ανακοιν")
}

var stopwords = func() map[string]bool {
	result := map[string]bool{}
	for _, word := range strings.Fields("a an and are for how in is of the to what για ειναι η και με ο οι ποιο πως σε στη στην στο τα τη την τι το του των") {
		result[word] = true
	}
	return result
}()

func lexicalQuery(query string) string {
	terms := []string{}
	for _, term := range strings.Fields(identity.Search(query)) {
		if !stopwords[term] {
			terms = append(terms, `"`+strings.ReplaceAll(term, `"`, `""`)+`"`)
		}
	}
	return strings.Join(terms, " OR ")
}
func academicYear(stamp string) *string {
	if len(stamp) < 10 {
		return nil
	}
	date, err := time.Parse(time.DateOnly, stamp[:10])
	if err != nil {
		return nil
	}
	year := date.Year()
	if date.Month() < 9 {
		year--
	}
	value := fmt.Sprintf("%d-%02d", year, (year+1)%100)
	return &value
}
