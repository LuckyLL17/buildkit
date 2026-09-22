package progress

import (
	"context"
	"io"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/stretchr/testify/require"
)

func TestMultiReaderStateReader(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	pr, pwctx, cancelProgress := NewContext(ctx)
	mr := NewMultiReader(pr)
	pw, _, _ := NewFromContext(pwctx)

	// Prime the MultiReader with an active reader so that it starts
	// collecting events before the state reader attaches.
	prime := mr.Reader(ctx)
	var primeCount atomic.Int32
	primeDone := make(chan struct{})
	go func() {
		defer close(primeDone)
		for {
			p, err := prime.Read(ctx)
			if err != nil {
				return
			}
			primeCount.Add(int32(len(p)))
		}
	}()

	require.NoError(t, pw.Write("a", 1))
	require.NoError(t, pw.Write("b", 2))
	require.Eventually(t, func() bool {
		return primeCount.Load() == 2
	}, 5*time.Second, time.Millisecond)

	stateReader, state := mr.StateReader(ctx)
	require.Len(t, state, 2)
	require.ElementsMatch(t, []string{"a", "b"}, []string{state[0].ID, state[1].ID})

	// Events written after attachment must only be delivered through the
	// reader (no duplicates from the captured state).
	require.NoError(t, pw.Write("c", 3))
	p, err := stateReader.Read(ctx)
	require.NoError(t, err)
	require.Len(t, p, 1)
	require.Equal(t, "c", p[0].ID)

	require.NoError(t, pw.Close())
	cancelProgress(errors.WithStack(context.Canceled))

	if _, err := stateReader.Read(ctx); !errors.Is(err, io.EOF) {
		require.Failf(t, "expected EOF", "got %v", err)
	}
	<-primeDone
}
