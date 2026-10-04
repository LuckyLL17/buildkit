package history

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"
	"time"

	controlapi "github.com/moby/buildkit/api/services/control"
	digest "github.com/opencontainers/go-digest"
	"github.com/stretchr/testify/require"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func testQueue(t *testing.T) (*Queue, *bolt.DB) {
	t.Helper()
	bdb, err := bolt.Open(filepath.Join(t.TempDir(), "history.db"), 0o600, nil)
	require.NoError(t, err)
	t.Cleanup(func() { bdb.Close() })

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(recordsBucket)); err != nil {
			return err
		}
		_, err := tx.CreateBucketIfNotExists([]byte(eventsBucket))
		return err
	}))

	h := &Queue{
		ps: &pubsub[*controlapi.BuildHistoryEvent]{
			m: map[*channel[*controlapi.BuildHistoryEvent]]struct{}{},
		},
		active:     map[string]*controlapi.BuildHistoryRecord{},
		refs:       map[string]int{},
		deleted:    map[string]struct{}{},
		finalizers: map[string]*finalizer{},
	}
	h.opt.DB = bdb
	return h, bdb
}

func completeRecord(ref string, created time.Time, completed time.Time) *controlapi.BuildHistoryRecord {
	return &controlapi.BuildHistoryRecord{
		Ref:         ref,
		CreatedAt:   timestamppb.New(created),
		CompletedAt: timestamppb.New(completed),
	}
}

func journalEvent(t *testing.T, bdb *bolt.DB, ev *controlapi.BuildHistoryEvent, tm time.Time) *controlapi.BuildHistoryEvent {
	t.Helper()
	var out *controlapi.BuildHistoryEvent
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		var err error
		out, err = appendJournal(tx, ev, tm)
		return err
	}))
	return out
}

func storeRecord(t *testing.T, bdb *bolt.DB, rec *controlapi.BuildHistoryRecord) {
	t.Helper()
	dt, err := rec.MarshalVT()
	require.NoError(t, err)
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(recordsBucket)).Put([]byte(rec.Ref), dt)
	}))
}

func TestCursorEncodeDecode(t *testing.T) {
	dgst := digest.FromString("test-digest")
	c := encodeCursor(42, dgst)

	seq, gotDgst, err := decodeCursor(c)
	require.NoError(t, err)
	require.Equal(t, uint64(42), seq)
	require.Equal(t, dgst, gotDgst)

	badPayload := func(c eventCursor) string {
		dt, _ := json.Marshal(c)
		return base64.RawURLEncoding.EncodeToString(dt)
	}
	for _, in := range []string{
		"garbage",
		"bkh_" + "!!!",
		"bkh_" + badPayload(eventCursor{Version: 2, Seq: 1, Digest: dgst.String()}),
		"bkh_" + badPayload(eventCursor{Version: cursorVersion, Seq: 0, Digest: dgst.String()}),
		"bkh_" + badPayload(eventCursor{Version: cursorVersion, Seq: 1, Digest: "not-a-digest"}),
	} {
		_, _, err := decodeCursor(in)
		require.Error(t, err)
	}
}

func TestFrameRoundTrip(t *testing.T) {
	now := time.Now()
	ev := &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_STARTED,
		Record: completeRecord("ref1", now, now.Add(time.Second)),
	}
	frame, err := marshalFrame(7, now, ev)
	require.NoError(t, err)

	seq, tm, got, err := unmarshalFrame(frame)
	require.NoError(t, err)
	require.Equal(t, uint64(7), seq)
	require.Equal(t, now.UnixNano(), tm.UnixNano())
	require.True(t, proto.Equal(ev, got))

	_, err = marshalFrame(1, now, &controlapi.BuildHistoryEvent{Cursor: "x"})
	require.Error(t, err)

	_, _, _, err = unmarshalFrame(frame[:5])
	require.Error(t, err)
}

func collectEvents(t *testing.T, h *Queue, req *controlapi.BuildHistoryRequest) []*controlapi.BuildHistoryEvent {
	t.Helper()
	var out []*controlapi.BuildHistoryEvent
	require.NoError(t, h.Listen(context.Background(), req, func(e *controlapi.BuildHistoryEvent) error {
		out = append(out, e)
		return nil
	}))
	return out
}

func TestListenSnapshotSemantics(t *testing.T) {
	h, bdb := testQueue(t)
	base := time.Now().Add(-time.Hour)

	recA := completeRecord("a", base, base.Add(time.Minute))
	recB := completeRecord("b", base.Add(2*time.Minute), base.Add(3*time.Minute))
	recC := completeRecord("c", base.Add(4*time.Minute), base.Add(5*time.Minute))
	for _, rec := range []*controlapi.BuildHistoryRecord{recA, recB, recC} {
		storeRecord(t, bdb, rec)
		journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
			Type:   controlapi.BuildHistoryEventType_COMPLETE,
			Record: rec,
		}, rec.CompletedAt.AsTime())
	}

	activeRec := completeRecord("d", time.Now(), time.Time{})
	journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_STARTED,
		Record: activeRec,
	}, activeRec.CreatedAt.AsTime())
	h.active["d"] = activeRec

	t.Run("full snapshot", func(t *testing.T) {
		events := collectEvents(t, h, &controlapi.BuildHistoryRequest{EarlyExit: true})
		require.Len(t, events, 4)
		require.Equal(t, controlapi.BuildHistoryEventType_STARTED, events[0].Type)
		require.Equal(t, "d", events[0].Record.Ref)
		require.NotEmpty(t, events[0].Cursor)
		require.Equal(t, []string{"a", "b", "c"}, []string{
			events[1].Record.Ref, events[2].Record.Ref, events[3].Record.Ref,
		})
		for _, e := range events {
			require.NotEmpty(t, e.Cursor)
		}
	})

	t.Run("active only", func(t *testing.T) {
		events := collectEvents(t, h, &controlapi.BuildHistoryRequest{EarlyExit: true, ActiveOnly: true})
		require.Len(t, events, 1)
		require.Equal(t, "d", events[0].Record.Ref)
	})

	t.Run("ref filter", func(t *testing.T) {
		events := collectEvents(t, h, &controlapi.BuildHistoryRequest{EarlyExit: true, Ref: "b"})
		require.Len(t, events, 1)
		require.Equal(t, "b", events[0].Record.Ref)
	})

	t.Run("limit newest first", func(t *testing.T) {
		events := collectEvents(t, h, &controlapi.BuildHistoryRequest{EarlyExit: true, Limit: 1})
		require.Len(t, events, 2)
		require.Equal(t, "d", events[0].Record.Ref)
		require.Equal(t, "c", events[1].Record.Ref)
	})
}

func TestListenIncremental(t *testing.T) {
	h, bdb := testQueue(t)
	base := time.Now().Add(-time.Hour)

	recA := completeRecord("a", base, base.Add(time.Minute))
	storeRecord(t, bdb, recA)
	cursorEvent := journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_COMPLETE,
		Record: recA,
	}, recA.CompletedAt.AsTime())

	recB := completeRecord("b", base.Add(2*time.Minute), base.Add(3*time.Minute))
	storeRecord(t, bdb, recB)
	journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_COMPLETE,
		Record: recB,
	}, recB.CompletedAt.AsTime())

	recC := completeRecord("c", base.Add(4*time.Minute), base.Add(5*time.Minute))
	storeRecord(t, bdb, recC)
	journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_COMPLETE,
		Record: recC,
	}, recC.CompletedAt.AsTime())

	journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_DELETED,
		Record: &controlapi.BuildHistoryRecord{Ref: "a"},
	}, time.Now())

	t.Run("catch up", func(t *testing.T) {
		events := collectEvents(t, h, &controlapi.BuildHistoryRequest{
			EarlyExit: true,
			Cursor:    cursorEvent.Cursor,
		})
		require.Len(t, events, 3)
		require.Equal(t, "b", events[0].Record.Ref)
		require.Equal(t, controlapi.BuildHistoryEventType_COMPLETE, events[0].Type)
		require.Equal(t, "c", events[1].Record.Ref)
		require.Equal(t, controlapi.BuildHistoryEventType_DELETED, events[2].Type)
		require.Equal(t, "a", events[2].Record.Ref)
		for _, e := range events {
			require.NotEmpty(t, e.Cursor)
		}

		// retrying the same cursor yields the same boundary
		retry := collectEvents(t, h, &controlapi.BuildHistoryRequest{
			EarlyExit: true,
			Cursor:    cursorEvent.Cursor,
		})
		require.Equal(t, eventRefs(events), eventRefs(retry))
	})

	t.Run("limit pages", func(t *testing.T) {
		page1 := collectEvents(t, h, &controlapi.BuildHistoryRequest{
			EarlyExit: true,
			Cursor:    cursorEvent.Cursor,
			Limit:     2,
		})
		require.Equal(t, []string{"b", "c"}, eventRefs(page1))

		page2 := collectEvents(t, h, &controlapi.BuildHistoryRequest{
			EarlyExit: true,
			Cursor:    page1[1].Cursor,
		})
		require.Equal(t, []string{"a"}, eventRefs(page2))
		require.Equal(t, controlapi.BuildHistoryEventType_DELETED, page2[0].Type)
	})

	t.Run("filter", func(t *testing.T) {
		events := collectEvents(t, h, &controlapi.BuildHistoryRequest{
			EarlyExit: true,
			Cursor:    cursorEvent.Cursor,
			Filter:    []string{"ref==c"},
		})
		require.Equal(t, []string{"c"}, eventRefs(events))
	})

	t.Run("active only skips complete", func(t *testing.T) {
		events := collectEvents(t, h, &controlapi.BuildHistoryRequest{
			EarlyExit:  true,
			Cursor:     cursorEvent.Cursor,
			ActiveOnly: true,
		})
		require.Equal(t, []string{"a"}, eventRefs(events))
		require.Equal(t, controlapi.BuildHistoryEventType_DELETED, events[0].Type)
	})
}

func eventRefs(events []*controlapi.BuildHistoryEvent) []string {
	out := make([]string, 0, len(events))
	for _, e := range events {
		out = append(out, e.Record.Ref)
	}
	return out
}

func TestInvalidCursor(t *testing.T) {
	h, _ := testQueue(t)

	err := h.Listen(context.Background(), &controlapi.BuildHistoryRequest{Cursor: "bad-cursor"}, nil)
	require.Equal(t, codes.InvalidArgument, status.Code(err))
}

func TestExpiredCursor(t *testing.T) {
	h, bdb := testQueue(t)

	rec := completeRecord("a", time.Now().Add(-time.Hour), time.Now().Add(-time.Hour+time.Minute))
	storeRecord(t, bdb, rec)
	ev := journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_COMPLETE,
		Record: rec,
	}, rec.CompletedAt.AsTime())

	anchorSeq, _, err := decodeCursor(ev.Cursor)
	require.NoError(t, err)

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(eventsBucket)).Delete(sequenceKey(anchorSeq))
	}))

	err = h.Listen(context.Background(), &controlapi.BuildHistoryRequest{Cursor: ev.Cursor}, nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
	require.Contains(t, err.Error(), "resync without a cursor")

	// digest mismatch: replace the anchor frame with a different payload
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return tx.Bucket([]byte(eventsBucket)).Put(sequenceKey(anchorSeq), []byte("different-frame"))
	}))
	err = h.Listen(context.Background(), &controlapi.BuildHistoryRequest{Cursor: ev.Cursor}, nil)
	require.Equal(t, codes.FailedPrecondition, status.Code(err))
}

func TestValidateCursorAnchor(t *testing.T) {
	_, bdb := testQueue(t)
	now := time.Now()
	ev := journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_STARTED,
		Record: completeRecord("a", now, now),
	}, now)

	seq, dgst, err := decodeCursor(ev.Cursor)
	require.NoError(t, err)

	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		return validateCursorAnchor(tx, seq, dgst)
	}))
}

func TestPruneJournal(t *testing.T) {
	_, bdb := testQueue(t)
	old := time.Now().Add(-48 * time.Hour)
	recent := time.Now()

	recOld := completeRecord("old", old, old.Add(time.Minute))
	storeRecord(t, bdb, recOld)
	evOld := journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_COMPLETE,
		Record: recOld,
	}, old.Add(time.Minute))

	recNew := completeRecord("new", recent.Add(-time.Minute), recent)
	storeRecord(t, bdb, recNew)
	evNew := journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_COMPLETE,
		Record: recNew,
	}, recent)

	deleted := journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_DELETED,
		Record: &controlapi.BuildHistoryRecord{Ref: "gone"},
	}, old.Add(2*time.Hour))

	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		return pruneJournal(tx, map[string]struct{}{"new": {}}, time.Now().Add(-24*time.Hour))
	}))

	oldSeq, _, _ := decodeCursor(evOld.Cursor)
	newSeq, _, _ := decodeCursor(evNew.Cursor)
	delSeq, _, _ := decodeCursor(deleted.Cursor)

	require.NoError(t, bdb.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(eventsBucket))
		require.Nil(t, b.Get(sequenceKey(oldSeq)))
		require.Nil(t, b.Get(sequenceKey(delSeq)))
		require.NotNil(t, b.Get(sequenceKey(newSeq)))
		return nil
	}))
}

func TestListenContinuousLive(t *testing.T) {
	h, bdb := testQueue(t)
	now := time.Now()
	recA := completeRecord("a", now.Add(-time.Minute), now)
	storeRecord(t, bdb, recA)
	evA := journalEvent(t, bdb, &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_COMPLETE,
		Record: recA,
	}, now)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	got := make(chan *controlapi.BuildHistoryEvent, 8)
	go func() {
		err := h.Listen(ctx, &controlapi.BuildHistoryRequest{Cursor: evA.Cursor}, func(e *controlapi.BuildHistoryEvent) error {
			got <- e
			return nil
		})
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Logf("unexpected listen error: %v", err)
		}
	}()

	recB := completeRecord("b", now, now.Add(time.Minute))
	storeRecord(t, bdb, recB)
	var evB *controlapi.BuildHistoryEvent
	require.NoError(t, bdb.Update(func(tx *bolt.Tx) error {
		var err error
		evB, err = appendJournal(tx, &controlapi.BuildHistoryEvent{
			Type:   controlapi.BuildHistoryEventType_COMPLETE,
			Record: recB,
		}, recB.CompletedAt.AsTime())
		return err
	}))
	h.ps.Send(evB)

	select {
	case e := <-got:
		require.Equal(t, "b", e.Record.Ref)
		require.NotEmpty(t, e.Cursor)
	case <-time.After(5 * time.Second):
		t.Fatal("did not receive live event")
	}
}
