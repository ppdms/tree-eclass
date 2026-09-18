package workflow

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"

	"tree-eclass/internal/domain/identity"
	"tree-eclass/internal/infrastructure/platform"
	"tree-eclass/internal/integrations/parser"
)

func TestNativePackagedParserRelocates(t *testing.T) {
	base := os.Getenv("TREE_TEST_PARSER_BASE")
	if base == "" {
		t.Skip("set TREE_TEST_PARSER_BASE to the prepared locked parser distribution")
	}
	t.Parallel()
	var meta parserDistribution
	if err := platform.ReadJSON(filepath.Join(base, "distribution.json"), &meta); err != nil {
		t.Fatal(err)
	}
	if err := verifyParserDistribution(base, meta.RequirementsSHA); err != nil {
		t.Fatal(err)
	}
	repo, err := filepath.Abs("../../../..")
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	stage := filepath.Join(root, "temporary-build")
	if err = os.Mkdir(stage, 0700); err != nil {
		t.Fatal(err)
	}
	if err = cloneArtifact(filepath.Join(base, "python"), filepath.Join(stage, ".python")); err != nil {
		t.Fatal(err)
	}
	if err = copyParserSource(repo, stage); err != nil {
		t.Fatal(err)
	}
	if os.Getenv("TREE_NATIVE_TOOLS_TEST") == "1" {
		packageFixtureHelpers(t, stage)
	}
	final := filepath.Join(root, "relocated-release")
	if err = os.Rename(stage, final); err != nil {
		t.Fatal(err)
	}
	python := pythonPath(final, "stable")
	fixtures := t.TempDir()
	cmd := exec.Command(python, "-B", "-c", parserFixtureScript, fixtures)
	cmd.Dir = final
	cmd.Env = []string{"PATH=/usr/bin:/bin", "HOME=" + fixtures, "TMPDIR=" + fixtures}
	if b, err := cmd.CombinedOutput(); err != nil {
		t.Fatal(string(b), err)
	}
	if err = os.WriteFile(filepath.Join(fixtures, "notes.pdf"), tinyPDF("synthetic fixture"), 0600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(fixtures, "notes.rar"), fixtureRAR(map[string][]byte{"notes.txt": []byte("Synthetic RAR fixture")}), 0600); err != nil {
		t.Fatal(err)
	}
	runner := parser.New(python, final, t.TempDir())
	if os.Getenv("TREE_NATIVE_TOOLS_TEST") == "1" {
		runner.ToolsRoot = filepath.Join(final, "runtime")
		runner.Tessdata = filepath.Join(final, "runtime/tessdata")
	}
	packagedParserExtractionChecks(t, runner, fixtures)
	packagedParserOCRChecks(t, runner, repo, fixtures)
	if runner.ToolsRoot != "" {
		bundledRenderingChecks(t, final, fixtures, runner)
	}
	if _, err = releaseFiles(final); err != nil {
		t.Fatal("packaged parser has external artifact dependencies", err)
	}
	if err = verifyParserDistribution(base, meta.RequirementsSHA); err != nil {
		t.Fatal("parser mutated its base distribution", err)
	}
}

func packagedParserExtractionChecks(t *testing.T, runner *parser.Runner, fixtures string) {
	t.Helper()
	for _, name := range []string{"notes.txt", "notes.html", "notes.docx", "notes.pptx", "notes.xlsx", "notes.pdf", "notes.ipynb", "notes.zip"} {
		var content strings.Builder
		err := runner.Run(
			t.Context(),
			parser.Request{
				Operation: "extract",
				Path:      filepath.Join(fixtures, name),
				Source: &parser.Source{
					CourseID:    1,
					CourseName:  "Synthetic",
					DisplayName: name,
					SourcePath:  "/" + name,
					SourceHash:  "fixture",
				},
			},
			func(r parser.Record) error {
				if r.Type == "unit" {
					content.WriteString(r.Text)
				}
				return nil
			},
		)
		if err != nil || !strings.Contains(strings.ToLower(content.String()), "synthetic") {
			t.Fatal("packaged extraction", name, content.String(), err)
		}
	}
	packagedArchiveChecks(t, runner, filepath.Join(fixtures, "notes.rar"), "Synthetic RAR fixture")
}

func packagedParserOCRChecks(t *testing.T, runner *parser.Runner, repo, fixtures string) {
	t.Helper()
	if models := os.Getenv("TREE_TEST_TESSDATA"); models != "" {
		if runner.Tessdata == "" {
			runner.Tessdata = models
		}
		var content strings.Builder
		err := runner.Run(
			t.Context(),
			parser.Request{
				Operation: "extract",
				Kind:      "image",
				Path:      filepath.Join(repo, "backend/internal/integrations/parser/testdata/greek-ocr.png"),
				Source: &parser.Source{
					CourseID:    1,
					CourseName:  "Synthetic",
					DisplayName: "greek.png",
					SourcePath:  "/greek.png",
					SourceHash:  "fixture",
					MIMEType:    "image/png",
				},
				Limits: map[string]any{"ocr_enabled": true, "ocr_languages": "ell+eng"},
			},
			func(r parser.Record) error {
				if r.Type == "unit" {
					content.WriteString(r.Text)
				}
				return nil
			},
		)
		if err != nil || !strings.Contains(identity.Search(content.String()), "δενδρα") {
			t.Fatal("packaged OCR", content.String(), err)
		}
	}
}

const parserFixtureScript = `import importlib,json,pathlib,pkgutil,sys,zipfile
import docx,pptx,openpyxl,pypdf,pdfminer,bs4,PIL,cryptography
import parser
for module in pkgutil.walk_packages(parser.__path__, 'parser.'):
    importlib.import_module(module.name)
root=pathlib.Path(sys.argv[1])
assert pathlib.Path(sys.prefix).resolve()==pathlib.Path.cwd()/'.python'
(root/'notes.txt').write_text('synthetic fixture Ελληνικά')
(root/'notes.html').write_text('<p>synthetic fixture</p>')
d=docx.Document();d.add_paragraph('synthetic fixture');d.save(root/'notes.docx')
p=pptx.Presentation();s=p.slides.add_slide(p.slide_layouts[1]);s.shapes.title.text='synthetic fixture';p.save(root/'notes.pptx')
w=openpyxl.Workbook();w.active['A1']='synthetic fixture';w.save(root/'notes.xlsx')
(root/'notes.ipynb').write_text(json.dumps({'cells':[{'cell_type':'markdown','source':['synthetic fixture']}],'metadata':{},'nbformat':4,'nbformat_minor':5}))
with zipfile.ZipFile(root/'notes.zip','w') as z:z.writestr('notes.txt','synthetic fixture')
`
