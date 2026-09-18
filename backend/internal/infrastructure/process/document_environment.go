package process

import (
	"os"
	"path/filepath"
)

// DocumentEnvironment keeps helper writes inside the owned job workspace. A
// packaged runtime resolves helpers and font configuration entirely inside itself.
func DocumentEnvironment(temp, tools string) []string {
	path := os.Getenv("PATH")
	if tools != "" {
		path = filepath.Join(tools, "bin") + ":/usr/bin:/bin"
	}
	env := []string{
		"PATH=" + path,
		"LANG=C.UTF-8",
		"TMPDIR=" + temp,
		"HOME=" + temp,
		"XDG_CACHE_HOME=" + filepath.Join(temp, "cache"),
	}
	if tools != "" {
		env = append(
			env,
			"FONTCONFIG_FILE="+filepath.Join(tools, "share/fontconfig/fonts.conf"),
			"FONTCONFIG_PATH="+filepath.Join(tools, "share/fontconfig"),
		)
	}
	return env
}
