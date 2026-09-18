package synchronization

import "strings"

func Diff(old, next Tree, root string) []Change {
	changes := []Change{}
	dirs := map[string]bool{}
	for _, d := range next.Directories {
		dirs[d.Path] = true
	}
	oldDirs := map[string]bool{}
	for _, d := range old.Directories {
		oldDirs[d.Path] = true
		if !dirs[d.Path] && d.Path != root {
			changes = append(changes, Change{Type: "deleted_directory", Path: relative(d.Path, root)})
		}
	}
	for _, d := range next.Directories {
		if !oldDirs[d.Path] && d.Path != root {
			changes = append(changes, Change{Type: "added_directory", Path: relative(d.Path, root)})
		}
	}
	previous := map[string]File{}
	for _, f := range old.Files {
		previous[f.Parent+"\x00"+f.URL] = f
	}
	seen := map[string]bool{}
	for _, f := range next.Files {
		key := f.Parent + "\x00" + f.URL
		seen[key] = true
		before, exists := previous[key]
		change := Change{Path: relative(f.Path, root), Name: f.Name, Redirect: f.Redirect}
		switch {
		case !exists:
			change.Type = "added_file"
		case before.MD5 != f.MD5 || before.Path != f.Path:
			change.Type = "modified_file"
			change.Previous = &before
			change.Current = &f
		default:
			continue
		}
		changes = append(changes, change)
	}
	for _, f := range old.Files {
		if !seen[f.Parent+"\x00"+f.URL] {
			changes = append(
				changes,
				Change{
					Type:     "deleted_file",
					Path:     relative(f.Path, root),
					Name:     f.Name,
					Redirect: f.Redirect,
					Previous: &f,
				},
			)
		}
	}
	return changes
}

func result(changes []Change) Result {
	r := Result{Changes: changes}
	for _, c := range changes {
		if c.Type == "added_file" {
			r.FilesAdded++
		}
		if c.Type == "modified_file" {
			r.FilesChanged++
		}
		if strings.HasPrefix(c.Type, "added_") {
			r.Added++
		}
		if strings.HasPrefix(c.Type, "deleted_") {
			r.Deleted++
		}
		if c.Type == "modified_file" {
			r.Modified++
		}
	}
	return r
}
