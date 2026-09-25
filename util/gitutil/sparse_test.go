package gitutil

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseSparseCheckoutPatterns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		patterns []string
		dirs     []string
		globs    []string
		cone     bool
	}{
		{
			name:     "single directory",
			patterns: []string{"cmd"},
			dirs:     []string{"cmd"},
			cone:     true,
		},
		{
			name:     "directories normalized",
			patterns: []string{"/cmd/", "./a/b//", "a/b", "a//b"},
			dirs:     []string{"a/b", "cmd"},
			cone:     true,
		},
		{
			name:     "directories sorted and deduplicated",
			patterns: []string{"b", "a", "a/"},
			dirs:     []string{"a", "b"},
			cone:     true,
		},
		{
			name:     "globs force non-cone",
			patterns: []string{"cmd", "**/*.go", "a/?.txt"},
			dirs:     []string{"cmd"},
			globs:    []string{"**/*.go", "a/?.txt"},
			cone:     false,
		},
		{
			name:     "character class glob",
			patterns: []string{"src/[ch]/*"},
			globs:    []string{"src/[ch]/*"},
			cone:     false,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := ParseSparseCheckoutPatterns(tc.patterns)
			require.NoError(t, err)
			require.Equal(t, tc.dirs, sc.Directories)
			require.Equal(t, tc.globs, sc.Patterns)
			require.Equal(t, tc.cone, sc.Cone())
			require.True(t, sc.Enabled())
		})
	}
}

func TestParseSparseCheckoutPatternsErrors(t *testing.T) {
	t.Parallel()
	for _, p := range []string{
		"",
		"../etc",
		"a/../../etc",
		"a/../b",
		".",
		"/",
		`a\b`,
		"!foo",
		":(glob)foo",
		"-foo",
		"#comment",
		"a\n/b",
	} {
		_, err := ParseSparseCheckoutPatterns([]string{p})
		require.Error(t, err, "expected error for %q", p)
	}
}

func TestSparseCheckoutPatternArgs(t *testing.T) {
	t.Parallel()
	sc, err := ParseSparseCheckoutPatterns([]string{"cmd", "**/*.go"})
	require.NoError(t, err)
	require.Equal(t, []string{"/cmd/", "**/*.go"}, sc.PatternArgs())
}

func TestWritePatterns(t *testing.T) {
	t.Parallel()
	sc, err := ParseSparseCheckoutPatterns([]string{"cmd", "sub", "**/*.go"})
	require.NoError(t, err)
	var b bytes.Buffer
	require.NoError(t, sc.WritePatterns(&b))
	require.Equal(t, "/cmd/\n/sub/\n**/*.go\n", b.String())
}

func TestWriteConePatterns(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name     string
		patterns []string
		expected string
	}{
		{
			name:     "top level",
			patterns: []string{"cmd"},
			expected: "/*\n!/*/\n/cmd/\n",
		},
		{
			name:     "nested",
			patterns: []string{"a/b/c"},
			expected: "/*\n!/*/\n/a/\n!/a/*/\n/a/b/\n!/a/b/*/\n/a/b/c/\n",
		},
		{
			name:     "multiple merged",
			patterns: []string{"a/b/c", "cmd", "a/b/d"},
			expected: "/*\n!/*/\n/a/\n!/a/*/\n/a/b/\n!/a/b/*/\n/a/b/c/\n/a/b/d/\n/cmd/\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			sc, err := ParseSparseCheckoutPatterns(tc.patterns)
			require.NoError(t, err)
			var b bytes.Buffer
			require.NoError(t, sc.WriteConePatterns(&b))
			require.Equal(t, tc.expected, b.String())
		})
	}
}

func TestSparseCheckoutContainsPath(t *testing.T) {
	t.Parallel()
	sc, err := ParseSparseCheckoutPatterns([]string{"a/b", "cmd"})
	require.NoError(t, err)
	require.True(t, sc.ContainsPath("a/b"))
	require.True(t, sc.ContainsPath("a/b/c/d"))
	require.True(t, sc.ContainsPath("cmd"))
	require.True(t, sc.ContainsPath("cmd/x"))
	require.False(t, sc.ContainsPath("a"))
	require.False(t, sc.ContainsPath("a/c"))
	require.False(t, sc.ContainsPath("cmdx"))
	require.False(t, sc.ContainsPath("sub"))
}
