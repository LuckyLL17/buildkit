package git

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/snapshot"
	"github.com/moby/buildkit/solver/pb"
	"github.com/moby/buildkit/util/gitutil"
	"github.com/stretchr/testify/require"
)

func newSparseIdentifier(t *testing.T, repo gitRepoFixture, ref string, keepGitDir bool, patterns ...string) *GitIdentifier {
	t.Helper()
	sc, err := gitutil.ParseSparseCheckoutPatterns(patterns)
	require.NoError(t, err)
	return &GitIdentifier{
		Remote:         repo.mainURL,
		Ref:            ref,
		KeepGitDir:     keepGitDir,
		SparseCheckout: sc,
	}
}

func snapshotGitSource(t *testing.T, ctx context.Context, gs *Source, id *GitIdentifier) (cache.ImmutableRef, string, string) {
	t.Helper()
	g, err := gs.Resolve(ctx, id, nil, nil)
	require.NoError(t, err)
	key, pin, _, done, err := g.CacheKey(ctx, nil, 0)
	require.NoError(t, err)
	require.True(t, done)
	snap, err := g.Snapshot(ctx, nil)
	require.NoError(t, err)
	return snap, key, pin
}

func mountSnapshot(t *testing.T, ctx context.Context, snap cache.ImmutableRef) (string, func()) {
	t.Helper()
	mount, err := snap.Mount(ctx, true, nil)
	require.NoError(t, err)
	lm := snapshot.LocalMounter(mount)
	dir, err := lm.Mount()
	require.NoError(t, err)
	return dir, func() { lm.Unmount() }
}

func TestSparseCheckoutDirectoriesSHA1(t *testing.T) {
	testSparseCheckoutDirectories(t, false, "sha1")
}
func TestSparseCheckoutDirectoriesKeepGitDirSHA1(t *testing.T) {
	testSparseCheckoutDirectories(t, true, "sha1")
}
func TestSparseCheckoutDirectoriesSHA256(t *testing.T) {
	testSparseCheckoutDirectories(t, false, "sha256")
}
func TestSparseCheckoutDirectoriesKeepGitDirSHA256(t *testing.T) {
	testSparseCheckoutDirectories(t, true, "sha256")
}

func testSparseCheckoutDirectories(t *testing.T, keepGitDir bool, format string) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := logProgressStreams(t.Context(), t)
	gs := setupGitSource(t, t.TempDir())
	repo := setupGitRepo(t, format)

	id := newSparseIdentifier(t, repo, "master", keepGitDir, "subdir")
	snap, sparseKey, pin := snapshotGitSource(t, ctx, gs, id)
	defer snap.Release(t.Context())

	fullID := &GitIdentifier{Remote: repo.mainURL, Ref: "master", KeepGitDir: keepGitDir}
	fullSnap, fullKey, fullPin := snapshotGitSource(t, ctx, gs, fullID)
	defer fullSnap.Release(t.Context())

	require.Equal(t, pin, fullPin)
	require.NotEqual(t, fullKey, sparseKey, "sparse checkout must change the source identity")
	require.Contains(t, sparseKey, "(sparse=")

	dir, unmount := mountSnapshot(t, ctx, snap)
	defer unmount()

	// cone mode keeps root-level files and the selected directory recursively
	require.FileExists(t, filepath.Join(dir, "abc"))
	require.FileExists(t, filepath.Join(dir, "def"))
	require.FileExists(t, filepath.Join(dir, "subdir", "subfile"))

	if keepGitDir {
		require.DirExists(t, filepath.Join(dir, ".git"))
		dt, err := os.ReadFile(filepath.Join(dir, ".git", "info", "sparse-checkout"))
		require.NoError(t, err)
		require.Contains(t, string(dt), "/subdir/")
		// the sparse checkout clone still uses a shallow ref fetch
		require.FileExists(t, filepath.Join(dir, ".git", "shallow"))
	}

	// resolving the same rules again reuses the cached snapshot
	snap2, key2, _ := snapshotGitSource(t, ctx, gs, id)
	defer snap2.Release(t.Context())
	require.Equal(t, sparseKey, key2)
	require.Equal(t, snap.ID(), snap2.ID())

	// changing the rules invalidates the cached snapshot
	idOther := newSparseIdentifier(t, repo, "master", keepGitDir, "nonexistent-dir")
	snap3, key3, _ := snapshotGitSource(t, ctx, gs, idOther)
	defer snap3.Release(t.Context())
	require.NotEqual(t, sparseKey, key3)
	require.NotEqual(t, snap.ID(), snap3.ID())
}

func TestSparseCheckoutGlobSHA1(t *testing.T) {
	testSparseCheckoutGlob(t, "sha1")
}
func TestSparseCheckoutGlobSHA256(t *testing.T) {
	testSparseCheckoutGlob(t, "sha256")
}

func testSparseCheckoutGlob(t *testing.T, format string) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := logProgressStreams(t.Context(), t)
	gs := setupGitSource(t, t.TempDir())
	repo := setupGitRepo(t, format)

	// master contains root files abc, def, foo13 and subdir/subfile
	id := newSparseIdentifier(t, repo, "master", false, "[ad]*")
	snap, sparseKey, _ := snapshotGitSource(t, ctx, gs, id)
	defer snap.Release(t.Context())
	require.Contains(t, sparseKey, "(sparse=")

	dir, unmount := mountSnapshot(t, ctx, snap)
	defer unmount()

	require.FileExists(t, filepath.Join(dir, "abc"))
	require.FileExists(t, filepath.Join(dir, "def"))
	for _, p := range []string{"foo13", filepath.Join("subdir", "subfile")} {
		_, err := os.Lstat(filepath.Join(dir, p))
		require.ErrorIs(t, err, os.ErrNotExist, "%s must be excluded by the sparse rules", p)
	}
	require.NoDirExists(t, filepath.Join(dir, "subdir"))
}

func TestSparseCheckoutNestedGlobSHA1(t *testing.T) {
	testSparseCheckoutNestedGlob(t, "sha1")
}

func testSparseCheckoutNestedGlob(t *testing.T, format string) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := logProgressStreams(t.Context(), t)
	gs := setupGitSource(t, t.TempDir())
	repo := setupGitRepo(t, format)

	id := newSparseIdentifier(t, repo, "master", false, "**/subfile")
	snap, _, _ := snapshotGitSource(t, ctx, gs, id)
	defer snap.Release(t.Context())

	dir, unmount := mountSnapshot(t, ctx, snap)
	defer unmount()

	require.FileExists(t, filepath.Join(dir, "subdir", "subfile"))
	_, err := os.Lstat(filepath.Join(dir, "def"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Lstat(filepath.Join(dir, "abc"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestSparseCheckoutWithSubdirSHA1(t *testing.T) {
	testSparseCheckoutWithSubdir(t, "sha1")
}

func testSparseCheckoutWithSubdir(t *testing.T, format string) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := logProgressStreams(t.Context(), t)
	gs := setupGitSource(t, t.TempDir())
	repo := setupGitRepo(t, format)

	id := newSparseIdentifier(t, repo, "master", false, "subdir")
	id.Subdir = "subdir"
	snap, _, _ := snapshotGitSource(t, ctx, gs, id)
	defer snap.Release(t.Context())

	dir, unmount := mountSnapshot(t, ctx, snap)
	defer unmount()

	// sparse cone selects subdir, then the subdir extraction moves its
	// contents to the snapshot root
	require.FileExists(t, filepath.Join(dir, "subfile"))
	_, err := os.Lstat(filepath.Join(dir, "subdir"))
	require.ErrorIs(t, err, os.ErrNotExist)
	_, err = os.Lstat(filepath.Join(dir, "abc"))
	require.ErrorIs(t, err, os.ErrNotExist)
}

func TestSparseCheckoutMixedRulesSHA1(t *testing.T) {
	testSparseCheckoutMixedRules(t, "sha1")
}

// testSparseCheckoutMixedRules combines a directory with a glob, which forces
// non-cone mode and renders directory rules as anchored patterns.
func testSparseCheckoutMixedRules(t *testing.T, format string) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := logProgressStreams(t.Context(), t)
	gs := setupGitSource(t, t.TempDir())
	repo := setupGitRepo(t, format)

	id := newSparseIdentifier(t, repo, "master", false, "subdir")
	// a glob entry forces non-cone for the whole set
	id.SparseCheckout.Patterns = append(id.SparseCheckout.Patterns, "def*")
	require.False(t, id.SparseCheckout.Cone())
	snap, _, _ := snapshotGitSource(t, ctx, gs, id)
	defer snap.Release(t.Context())

	dir, unmount := mountSnapshot(t, ctx, snap)
	defer unmount()

	require.FileExists(t, filepath.Join(dir, "subdir", "subfile"))
	require.FileExists(t, filepath.Join(dir, "def"))
	for _, p := range []string{"abc", "foo13"} {
		_, err := os.Lstat(filepath.Join(dir, p))
		require.ErrorIs(t, err, os.ErrNotExist)
	}
}

func TestSparseCheckoutSubmoduleSHA1(t *testing.T) {
	testSparseCheckoutSubmodule(t, false, "sha1")
}
func TestSparseCheckoutSubmoduleKeepGitDirSHA1(t *testing.T) {
	testSparseCheckoutSubmodule(t, true, "sha1")
}

func testSparseCheckoutSubmodule(t *testing.T, keepGitDir bool, format string) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := logProgressStreams(t.Context(), t)
	gs := setupGitSource(t, t.TempDir())
	repo := setupGitRepo(t, format)

	// cone including the submodule: the submodule is initialized
	id := newSparseIdentifier(t, repo, "feature", keepGitDir, "sub")
	snap, _, _ := snapshotGitSource(t, ctx, gs, id)
	defer snap.Release(t.Context())

	dir, unmount := mountSnapshot(t, ctx, snap)
	defer unmount()

	require.FileExists(t, filepath.Join(dir, "sub", "subfile"))
	require.FileExists(t, filepath.Join(dir, "ghi"))
}

func TestSparseCheckoutSubmoduleExcludedSHA1(t *testing.T) {
	testSparseCheckoutSubmoduleExcluded(t, "sha1")
}

func testSparseCheckoutSubmoduleExcluded(t *testing.T, format string) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := logProgressStreams(t.Context(), t)
	gs := setupGitSource(t, t.TempDir())
	repo := setupGitRepo(t, format)

	// non-cone rule selecting only ghi: the submodule must not be initialized
	id := newSparseIdentifier(t, repo, "feature", false, "ghi*")
	snap, _, _ := snapshotGitSource(t, ctx, gs, id)
	defer snap.Release(t.Context())

	dir, unmount := mountSnapshot(t, ctx, snap)
	defer unmount()

	require.FileExists(t, filepath.Join(dir, "ghi"))
	require.NoDirExists(t, filepath.Join(dir, "sub"))
}

// TestSparseCheckoutSharedBareRepoClean verifies that a sparse checkout does
// not leak state into the shared bare repository: a later full checkout of
// the same remote still materializes every file and initializes submodules.
func TestSparseCheckoutSharedBareRepoCleanSHA1(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("Depends on unimplemented containerd bind-mount support on Windows")
	}

	t.Parallel()
	ctx := logProgressStreams(t.Context(), t)
	gs := setupGitSource(t, t.TempDir())
	repo := setupGitRepo(t, "sha1")

	sparseID := newSparseIdentifier(t, repo, "feature", false, "ghi*")
	sparseSnap, _, _ := snapshotGitSource(t, ctx, gs, sparseID)
	sparseDir, unmount := mountSnapshot(t, ctx, sparseSnap)
	require.FileExists(t, filepath.Join(sparseDir, "ghi"))
	require.NoDirExists(t, filepath.Join(sparseDir, "sub"))
	unmount()
	sparseSnap.Release(t.Context())

	fullID := &GitIdentifier{Remote: repo.mainURL, Ref: "feature"}
	fullSnap, _, _ := snapshotGitSource(t, ctx, gs, fullID)
	defer fullSnap.Release(t.Context())
	fullDir, unmountFull := mountSnapshot(t, ctx, fullSnap)
	defer unmountFull()

	require.FileExists(t, filepath.Join(fullDir, "ghi"))
	require.FileExists(t, filepath.Join(fullDir, "abc"))
	require.FileExists(t, filepath.Join(fullDir, "sub", "subfile"))
}

func TestSparseCheckoutIdentifierAttr(t *testing.T) {
	t.Parallel()
	gs, err := NewSource(Opt{})
	require.NoError(t, err)

	for _, tc := range []struct {
		name  string
		attrs map[string]string
		err   string
	}{
		{
			name: "invalid json",
			attrs: map[string]string{
				pb.AttrGitSparseCheckout: "{not-json",
			},
			err: "failed to parse git.sparsecheckout",
		},
		{
			name: "invalid pattern",
			attrs: map[string]string{
				pb.AttrGitSparseCheckout: `["../outside"]`,
			},
			err: "parent directory references",
		},
		{
			name: "checkoutbundle incompatible",
			attrs: map[string]string{
				pb.AttrGitSparseCheckout: `["cmd"]`,
				pb.AttrGitCheckoutBundle: "true",
				pb.AttrGitChecksum:       "1111111111111111111111111111111111111111",
				pb.AttrGitBundle:         "docker-image+blob://example.com/repo@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
			},
			err: "incompatible with git.sparsecheckout",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := gs.Identifier("git", "https://example.com/repo.git#master", tc.attrs, nil)
			require.Error(t, err)
			require.ErrorContains(t, err, tc.err)
		})
	}

	t.Run("valid", func(t *testing.T) {
		dt, _ := json.Marshal([]string{"cmd/", "**/*.go"})
		id, err := gs.Identifier("git", "https://example.com/repo.git#master", map[string]string{
			pb.AttrGitSparseCheckout: string(dt),
		}, nil)
		require.NoError(t, err)
		gid := id.(*GitIdentifier)
		require.Equal(t, []string{"cmd"}, gid.SparseCheckout.Directories)
		require.Equal(t, []string{"**/*.go"}, gid.SparseCheckout.Patterns)
		require.False(t, gid.SparseCheckout.Cone())
	})
}
