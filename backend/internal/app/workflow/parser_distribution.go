package workflow

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"

	"tree-eclass/internal/domain/platform"
)

type parserDistribution struct {
	PythonVersion, PythonBuild, RequirementsSHA string
	Files                                       map[string]string
}

func (c *Controller) prepareParser(ctx context.Context, requirements string) (string, error) {
	data, err := os.ReadFile(requirements)
	if err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	root := filepath.Join(c.Root, "tools", "parser-"+pythonVersion+"-"+pythonBuild+"-"+digest[:16])
	if _, err = os.Lstat(root); err == nil {
		return root, verifyParserDistribution(root, digest)
	} else if !errors.Is(err, os.ErrNotExist) {
		return "", err
	}
	if err = c.space(); err != nil {
		return "", err
	}
	if err = verifyPythonBase(c.pythonBase()); err != nil {
		return "", err
	}
	temp, err := os.MkdirTemp(filepath.Dir(root), ".parser-*")
	if err != nil {
		return "", err
	}
	defer os.RemoveAll(temp)
	pythonRoot := filepath.Join(temp, "python")
	if err = cloneArtifact(filepath.Join(c.pythonBase(), "python"), pythonRoot); err != nil {
		return "", err
	}
	// Use the exact bytes whose hash names this distribution, even if the editable
	// requirements file changes while installation is running.
	lock := filepath.Join(temp, "requirements.txt")
	if err = os.WriteFile(lock, data, 0600); err != nil {
		return "", err
	}
	if err = c.run(ctx, temp, "uv", "pip", "install", "--python", filepath.Join(pythonRoot, "bin/python3.14"), "--system", "--require-hashes", "--link-mode=copy", "-r", lock); err != nil {
		return "", err
	}
	if err = trimPython(pythonRoot); err != nil {
		return "", err
	}
	files, err := releaseFiles(pythonRoot)
	if err != nil {
		return "", err
	}
	if err = platform.WriteJSON(
		filepath.Join(temp, "distribution.json"),
		parserDistribution{pythonVersion, pythonBuild, digest, files},
	); err != nil {
		return "", err
	}
	if err = platform.SyncTree(temp); err != nil {
		return "", err
	}
	if err = os.Rename(temp, root); err != nil {
		return "", err
	}
	return root, platform.SyncDir(filepath.Dir(root))
}

func verifyParserDistribution(root, digest string) error {
	var distribution parserDistribution
	if err := platform.ReadJSON(filepath.Join(root, "distribution.json"), &distribution); err != nil {
		return err
	}
	if distribution.PythonVersion != pythonVersion || distribution.PythonBuild != pythonBuild ||
		distribution.RequirementsSHA != digest {
		return errors.New("parser distribution identity changed")
	}
	data, err := os.ReadFile(filepath.Join(root, "requirements.txt"))
	if err != nil {
		return err
	}
	if fmt.Sprintf("%x", sha256.Sum256(data)) != digest {
		return errors.New("parser dependency lock changed")
	}
	return verifyRelease(filepath.Join(root, "python"), Release{Files: distribution.Files})
}

func (c *Controller) setupParser(ctx context.Context) (err error) {
	if err = installPythonBase(ctx, c.pythonBase()); err != nil {
		return err
	}
	clean := c.releaseBuildCache()
	defer func() { err = errors.Join(err, clean()) }()
	_, err = c.prepareParser(ctx, filepath.Join(c.Repo, "requirements-parser.txt"))
	return err
}

func (c *Controller) preparedParser() (string, error) {
	data, err := os.ReadFile(filepath.Join(c.Repo, "requirements-parser.txt"))
	if err != nil {
		return "", err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(data))
	root := filepath.Join(c.Root, "tools", "parser-"+pythonVersion+"-"+pythonBuild+"-"+digest[:16])
	return root, verifyParserDistribution(root, digest)
}

func (c *Controller) setupDevelopment(ctx context.Context) (err error) {
	bun, err := exec.LookPath("bun")
	if err != nil {
		return err
	}
	c.Config.Bun = bun
	base, err := c.preparedParser()
	if err != nil {
		return err
	}
	c.Config.ParserPython = filepath.Join(base, "python/bin/python3.14")
	clean := c.releaseBuildCache()
	defer func() { err = errors.Join(err, clean()) }()
	return c.run(ctx, filepath.Join(c.Repo, "frontend"), c.Config.Bun, "install", "--frozen-lockfile")
}
