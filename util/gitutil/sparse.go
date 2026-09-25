package gitutil

import (
	"io"
	"path"
	"sort"
	"strings"

	"github.com/pkg/errors"
)

const sparseCheckoutGlobChars = `*?[`

// SparseCheckout holds a canonical set of git sparse-checkout rules.
//
// Directories are root-relative directory paths without leading or trailing
// slashes ("a/b"). They are always included recursively. Patterns keep the
// user-provided git sparse-checkout (non-cone) syntax, e.g. "**/*.go" or
// "/cmd". Parent directories of included paths are retained by git itself.
//
// When all rules are plain directories git's optimized cone mode can be used;
// any glob pattern forces non-cone mode for the whole rule set.
type SparseCheckout struct {
	Directories []string
	Patterns    []string
}

// Enabled reports whether any sparse-checkout rule is configured.
func (s SparseCheckout) Enabled() bool {
	return len(s.Directories) != 0 || len(s.Patterns) != 0
}

// Cone reports whether the rules can run in git's cone mode.
func (s SparseCheckout) Cone() bool {
	return s.Enabled() && len(s.Patterns) == 0
}

// ParseSparseCheckoutPatterns validates raw user rules and returns the
// canonical form. Plain directory paths are normalized into Directories;
// rules containing glob magic (*, ?, [) are kept in Patterns.
func ParseSparseCheckoutPatterns(patterns []string) (SparseCheckout, error) {
	var sc SparseCheckout
	dirs := make(map[string]struct{})
	globs := make(map[string]struct{})
	for _, p := range patterns {
		if err := validateSparseCheckoutPattern(p); err != nil {
			return sc, err
		}
		if strings.ContainsAny(p, sparseCheckoutGlobChars) {
			globs[p] = struct{}{}
			continue
		}
		d, err := normalizeSparseCheckoutDir(p)
		if err != nil {
			return sc, err
		}
		dirs[d] = struct{}{}
	}
	sc.Directories = sortedKeys(dirs)
	sc.Patterns = sortedKeys(globs)
	return sc, nil
}

func sortedKeys(m map[string]struct{}) []string {
	if len(m) == 0 {
		return nil
	}
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func validateSparseCheckoutPattern(p string) error {
	if p == "" {
		return errors.New("empty git sparse-checkout pattern")
	}
	if strings.ContainsAny(p, "\x00\n\r") {
		return errors.Errorf("invalid git sparse-checkout pattern %q: contains control characters", p)
	}
	if strings.Contains(p, `\`) {
		return errors.Errorf("invalid git sparse-checkout pattern %q: backslashes are not supported", p)
	}
	switch p[0] {
	case '!':
		return errors.Errorf("invalid git sparse-checkout pattern %q: negation rules are not supported", p)
	case ':':
		return errors.Errorf("invalid git sparse-checkout pattern %q: pathspec magic is not supported", p)
	case '-':
		return errors.Errorf("invalid git sparse-checkout pattern %q", p)
	case '#':
		return errors.Errorf("invalid git sparse-checkout pattern %q: comments are not supported", p)
	}
	for _, part := range strings.Split(p, "/") {
		if part == ".." {
			return errors.Errorf("invalid git sparse-checkout pattern %q: parent directory references are not supported", p)
		}
	}
	return nil
}

func normalizeSparseCheckoutDir(p string) (string, error) {
	p = strings.TrimPrefix(p, "./")
	p = strings.Trim(p, "/")
	for _, part := range strings.Split(p, "/") {
		if part == "." {
			return "", errors.Errorf("invalid git sparse-checkout directory %q", p)
		}
	}
	d := path.Clean(p)
	if d == "." || d == "/" {
		return "", errors.Errorf("invalid git sparse-checkout directory %q: the repository root requires a full checkout", p)
	}
	return d, nil
}

// PatternArgs returns the rules as arguments for "git sparse-checkout set".
func (s SparseCheckout) PatternArgs() []string {
	args := make([]string, 0, len(s.Directories)+len(s.Patterns))
	for _, d := range s.Directories {
		args = append(args, "/"+d+"/")
	}
	args = append(args, s.Patterns...)
	return args
}

// WritePatterns writes the non-cone sparse-checkout rules file. Directory
// rules are anchored at the repository root and match recursively.
func (s SparseCheckout) WritePatterns(w io.Writer) error {
	var b strings.Builder
	for _, d := range s.Directories {
		b.WriteString("/")
		b.WriteString(d)
		b.WriteString("/\n")
	}
	for _, p := range s.Patterns {
		b.WriteString(p)
		b.WriteString("\n")
	}
	_, err := io.WriteString(w, b.String())
	return err
}

// WriteConePatterns writes the rules in the format git generates for cone
// mode. For each path component level it re-includes the directory and
// excludes its sibling subdirectories, e.g. for "a/b/c":
//
//	/a/
//	!/a/*/
//	/a/b/
//	!/a/b/*/
//	/a/b/c/
func (s SparseCheckout) WriteConePatterns(w io.Writer) error {
	lines := []string{"/*", "!/*/"}
	seen := map[string]bool{
		"/*":    true,
		"!/*/":  true,
	}
	add := func(line string) {
		if !seen[line] {
			seen[line] = true
			lines = append(lines, line)
		}
	}
	for _, d := range s.Directories {
		parts := strings.Split(d, "/")
		cur := ""
		for i, part := range parts {
			cur += "/" + part
			add(cur + "/")
			if i != len(parts)-1 {
				add("!" + cur + "/*/")
			}
		}
	}
	_, err := io.WriteString(w, strings.Join(lines, "\n")+"\n")
	return err
}

// ContainsPath reports whether a root-relative repository path lies inside
// a cone-mode directory rule.
func (s SparseCheckout) ContainsPath(p string) bool {
	for _, d := range s.Directories {
		if p == d || strings.HasPrefix(p, d+"/") {
			return true
		}
	}
	return false
}
