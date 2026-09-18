package eclass

import (
	"strings"
	"testing"
)

func TestDocumentLinksKeepNamesAndBoundCourse(t *testing.T) {
	data := `<h1>Έγγραφα</h1><table><tr><td><a class="fileURL" href="/modules/document/file.php?course=INF101&amp;download=/drive">Greek notes</a></td><td><a href="https://drive.google.com/file/d/abc_123/view">download</a></td></tr></table>
<a href="/modules/document/file.php?course=INF101&amp;download=/a.pdf">Δένδρα</a>
<a href="/modules/document/index.php?course=INF101&amp;openDir=/folder">Διαλέξεις</a>
<a href="/modules/document/index.php?course=INF101&amp;openDir=%2F">root</a>
<a href="/modules/document/index.php?course=INF999&amp;openDir=/other">Other course</a>
<a href="https://unrelated.invalid/modules/document/file.php?course=INF101">offsite</a>
<a href="/modules/document/file.php?course=INF101&amp;download=/a.pdf">duplicate</a>`
	links, err := ParseLinks([]byte(data), BaseURL+"/modules/document/index.php?course=INF101")
	if err != nil || len(links.Files) != 2 || len(links.Directories) != 1 {
		t.Fatalf("links: %#v %v", links, err)
	}
	if links.Files[0].Name != "Greek notes" || links.Files[1].Name != "Δένδρα" ||
		links.Directories[0].Name != "Διαλέξεις" {
		t.Fatal(links)
	}
	if _, err = ParseLinks([]byte(`<h1>Unavailable</h1>`), BaseURL); err == nil {
		t.Fatal("error page accepted as empty course")
	}
}

func TestDocumentLinksExcludeTokenizedDownloadActions(t *testing.T) {
	data := `<h1>Έγγραφα</h1>
<a href="index.php?course=INF101&amp;download=folder-token"><i></i></a>
<a href="index.php?course=INF101&amp;openDir=/2026">2026</a>
<table><tr><td><a href="index.php?course=INF101&amp;download=request-token"><i></i></a></td>
<td><a class="fileURL" href="file.php/INF101/00-introduction.pdf">Introduction</a></td></tr></table>`
	links, err := ParseLinks([]byte(data), BaseURL+"/modules/document/index.php?course=INF101")
	if err != nil || len(links.Files) != 1 || len(links.Directories) != 1 {
		t.Fatalf("download actions became duplicate documents: %#v %v", links, err)
	}
	if links.Files[0].URL != BaseURL+"/modules/document/file.php/INF101/00-introduction.pdf" ||
		links.Files[0].Name != "Introduction" {
		t.Fatal("canonical document identity changed", links.Files)
	}
}

func TestAnnouncementIdentityAndDates(t *testing.T) {
	data := `<rss><channel><item><title> Νέα </title><link>https://eclass.aueb.gr/?an_id=98099</link><guid>Wed, 18 Feb 2026 17:48:07 +030098099</guid><pubDate>Wed, 18 Feb 2026 17:48:07 +0300</pubDate><description><![CDATA[<p>Καλημέρα</p>]]></description></item><item><link>https://aueb.gr/news/example</link></item></channel></rss>`
	items, err := ParseRSS([]byte(data))
	if err != nil || len(items) != 2 {
		t.Fatal(items, err)
	}
	if items[0].ID != "030098099" || items[0].Title != "Νέα" || *items[0].Published != "2026-02-18T17:48:07+03:00" ||
		items[0].Description != "<p>Καλημέρα</p>" {
		t.Fatal(items[0])
	}
	if items[1].ID != "https://aueb.gr/news/example" || items[1].Title != "Untitled" || items[1].Published != nil {
		t.Fatal(items[1])
	}
	if _, err = ParseRSS([]byte(`<rss></rss>`)); err == nil {
		t.Fatal("missing channel accepted")
	}
}

func TestExerciseListAndGreekDetail(t *testing.T) {
	data := `<table id="assignment_table_INF101"><tbody><tr><td><a href="/modules/work/index.php?course=INF101&amp;id=42">Εργασία</a><small class="text-muted">Ατομική</small></td><td>Αύριο 23:59<div>time left</div></td><td><i class="fa fa-check"></i></td><td>7</td></tr></tbody></table>`
	items, err := ParseExercises([]byte(data), BaseURL, 101)
	if err != nil || len(items) != 1 {
		t.Fatal(items, err)
	}
	ex := items[0]
	if ex.ID != "42" || ex.Deadline != "Αύριο 23:59" || ex.WorkType != "Ατομική" ||
		ex.SubmissionStatus != "submitted" ||
		ex.Grade != "7" {
		t.Fatal(ex)
	}
	field := func(label, value string) string {
		return `<li class="list-group-item"><div class="title-default">` + label + `:</div><div class="title-default-line-height">` + value + `</div></li>`
	}
	card := func(title, body string) string {
		return `<div class="panelCard"><div class="card-header"><h3>` + title + `</h3></div><div class="card-body">` + body + `</div></div>`
	}
	detail := card(
		"Στοιχεία εργασίας",
		field(
			"Περιγραφή",
			"<p>Δένδρα <strong>και γράφοι</strong></p>",
		)+field(
			"Ημερομηνία Έναρξης",
			"10 Σεπτεμβρίου 2026<div>remaining</div>",
		)+field(
			"Μέγιστη βαθμολογία",
			"10",
		)+field(
			"Αρχείο",
			`<a title="assignment.pdf" href="/modules/work/file.php?id=42">Download</a>`,
		),
	)
	detail += card(
		"Στοιχεία υποβολής",
		field("Βαθμός", "9")+field("Σχόλια βαθμολογητή", "Καλή εργασία")+field("Ημ/νία αποστολής", "11 Σεπτεμβρίου"),
	)
	if err = ParseExerciseDetail([]byte(detail), BaseURL, &ex); err != nil {
		t.Fatal(err)
	}
	if ex.Grade != "9" || ex.StartDate != "10 Σεπτεμβρίου 2026" ||
		!strings.Contains(ex.Description, "<strong>και γράφοι</strong>") ||
		ex.AssignmentFileName != "assignment.pdf" ||
		ex.GradeComments != "Καλή εργασία" {
		t.Fatal(ex)
	}
}

func TestExerciseListAcceptsDisabledModulePortfolio(t *testing.T) {
	data := `<table id="portfolio_lessons"><tr><td><a href="/courses/INF168/">Λειτουργικά Συστήματα</a></td></tr></table>`
	items, err := ParseExercises([]byte(data), BaseURL, 168)
	if err != nil || len(items) != 0 {
		t.Fatalf("disabled work module: %#v %v", items, err)
	}
	unrelated := `<table id="portfolio_lessons"><tr><td><a href="/courses/INF999/">Other course</a></td></tr></table>`
	if _, err = ParseExercises([]byte(unrelated), BaseURL, 168); err == nil {
		t.Fatal("unrelated portfolio page accepted as the requested course")
	}
}

func TestDriveURLPreservesAccessParameters(t *testing.T) {
	u, err := driveURL(
		"https://drive.google.com/file/d/abc_123/view?resourcekey=key&authuser=student%40example.invalid",
	)
	if err != nil || !strings.Contains(u, "resourcekey=key") ||
		!strings.Contains(u, "authuser=student%40example.invalid") ||
		!strings.Contains(u, "id=abc_123") {
		t.Fatal(u, err)
	}
	if _, err = driveURL("https://evil.invalid/open?id=abc"); err == nil {
		t.Fatal("non-Drive URL accepted")
	}
}
