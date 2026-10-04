package history

import (
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"strings"
	"time"

	controlapi "github.com/moby/buildkit/api/services/control"
	digest "github.com/opencontainers/go-digest"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	eventsBucket = "_events"

	cursorVersion = 1
	cursorPrefix  = "bkh_"

	frameSeqSize      = 8
	frameTimeSize     = 8
	frameLengthSize   = 4
	frameHeaderSize   = frameSeqSize + frameTimeSize + frameLengthSize
	journalInvalidMsg = "history cursor is invalid or has expired; the events are no longer available, resync without a cursor"
)

// eventCursor is the JSON payload of the opaque cursor tokens.
type eventCursor struct {
	Version int    `json:"v"`
	Seq     uint64 `json:"seq"`
	Digest  string `json:"digest"`
}

func encodeCursor(seq uint64, dgst digest.Digest) string {
	dt, _ := json.Marshal(eventCursor{
		Version: cursorVersion,
		Seq:     seq,
		Digest:  dgst.String(),
	})
	return cursorPrefix + base64.RawURLEncoding.EncodeToString(dt)
}

func decodeCursor(in string) (uint64, digest.Digest, error) {
	if !strings.HasPrefix(in, cursorPrefix) {
		return 0, "", errors.New("invalid history cursor: unknown format")
	}
	dt, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(in, cursorPrefix))
	if err != nil {
		return 0, "", errors.Wrap(err, "invalid history cursor")
	}
	var c eventCursor
	if err := json.Unmarshal(dt, &c); err != nil {
		return 0, "", errors.Wrap(err, "invalid history cursor")
	}
	if c.Version != cursorVersion {
		return 0, "", errors.Errorf("invalid history cursor: unsupported version %d", c.Version)
	}
	if c.Seq == 0 || c.Digest == "" {
		return 0, "", errors.New("invalid history cursor: missing sequence")
	}
	dgst, err := digest.Parse(c.Digest)
	if err != nil {
		return 0, "", errors.Wrap(err, "invalid history cursor")
	}
	return c.Seq, dgst, nil
}

func sequenceKey(seq uint64) []byte {
	key := make([]byte, frameSeqSize)
	binary.BigEndian.PutUint64(key, seq)
	return key
}

// marshalFrame serializes one journal entry. The returned frame is immutable;
// its digest anchors the cursor of the event.
func marshalFrame(seq uint64, tm time.Time, ev *controlapi.BuildHistoryEvent) ([]byte, error) {
	if ev.Cursor != "" {
		return nil, errors.New("journal event must not carry a cursor")
	}
	dt, err := ev.MarshalVT()
	if err != nil {
		return nil, err
	}
	frame := make([]byte, frameHeaderSize+len(dt))
	binary.BigEndian.PutUint64(frame[:frameSeqSize], seq)
	binary.BigEndian.PutUint64(frame[frameSeqSize:frameSeqSize+frameTimeSize], uint64(tm.UnixNano()))
	binary.BigEndian.PutUint32(frame[frameSeqSize+frameTimeSize:frameHeaderSize], uint32(len(dt)))
	copy(frame[frameHeaderSize:], dt)
	return frame, nil
}

func unmarshalFrame(frame []byte) (uint64, time.Time, *controlapi.BuildHistoryEvent, error) {
	if len(frame) < frameHeaderSize {
		return 0, time.Time{}, nil, errors.New("invalid journal frame: short header")
	}
	seq := binary.BigEndian.Uint64(frame[:frameSeqSize])
	tm := time.Unix(0, int64(binary.BigEndian.Uint64(frame[frameSeqSize:frameSeqSize+frameTimeSize])))
	size := binary.BigEndian.Uint32(frame[frameSeqSize+frameTimeSize : frameHeaderSize])
	if uint32(len(frame)-frameHeaderSize) < size {
		return 0, time.Time{}, nil, errors.New("invalid journal frame: short payload")
	}
	var ev controlapi.BuildHistoryEvent
	if err := ev.UnmarshalVT(frame[frameHeaderSize : frameHeaderSize+int(size)]); err != nil {
		return 0, time.Time{}, nil, err
	}
	return seq, tm, &ev, nil
}

// appendJournal assigns the next sequence to ev, persists its frame on tx and
// returns a copy with the cursor set. The caller decides the frame timestamp.
func appendJournal(tx *bolt.Tx, ev *controlapi.BuildHistoryEvent, tm time.Time) (*controlapi.BuildHistoryEvent, error) {
	b := tx.Bucket([]byte(eventsBucket))
	if b == nil {
		return nil, errors.Errorf("failed to retrieve bucket %s", eventsBucket)
	}
	seq, err := b.NextSequence()
	if err != nil {
		return nil, err
	}
	frame, err := marshalFrame(seq, tm, ev)
	if err != nil {
		return nil, err
	}
	if err := b.Put(sequenceKey(seq), frame); err != nil {
		return nil, err
	}
	out := ev.CloneVT()
	out.Cursor = encodeCursor(seq, digest.FromBytes(frame))
	return out, nil
}

// journalEntry is one scanned frame together with its parsed event.
type journalEntry struct {
	seq   uint64
	frame []byte
	ev    *controlapi.BuildHistoryEvent
}

// scanJournal walks frames in sequence order, starting strictly after afterSeq,
// and invokes fn for each one.
func scanJournal(tx *bolt.Tx, afterSeq uint64, fn func(journalEntry) error) error {
	b := tx.Bucket([]byte(eventsBucket))
	if b == nil {
		return nil
	}
	c := b.Cursor()
	key, frame := c.Seek(sequenceKey(afterSeq + 1))
	for key != nil {
		seq, _, ev, err := unmarshalFrame(frame)
		if err != nil {
			return err
		}
		if err := fn(journalEntry{seq: seq, frame: frame, ev: ev}); err != nil {
			return err
		}
		key, frame = c.Next()
	}
	return nil
}

// validateCursorAnchor verifies that the frame named by the cursor still
// exists and matches its digest.
func validateCursorAnchor(tx *bolt.Tx, seq uint64, dgst digest.Digest) error {
	b := tx.Bucket([]byte(eventsBucket))
	if b == nil {
		return status.Error(codes.FailedPrecondition, journalInvalidMsg)
	}
	frame := b.Get(sequenceKey(seq))
	if frame == nil || digest.FromBytes(frame) != dgst {
		return status.Error(codes.FailedPrecondition, journalInvalidMsg)
	}
	return nil
}

// matchHistoryFilter reports whether ev matches any of the parsed filter groups.
func matchHistoryFilter(fns []func(*controlapi.BuildHistoryEvent) bool, ev *controlapi.BuildHistoryEvent) bool {
	for _, fn := range fns {
		if fn(ev) {
			return true
		}
	}
	return false
}

// pruneJournal drops frames older than cutoff. Frames belonging to a ref that
// still has a completed record or an active build are retained until the next
// pass so that snapshot responses can keep issuing cursors for them.
func pruneJournal(tx *bolt.Tx, protectedRefs map[string]struct{}, cutoff time.Time) error {
	b := tx.Bucket([]byte(eventsBucket))
	if b == nil {
		return nil
	}

	protectedSeqs := map[uint64]struct{}{}
	if len(protectedRefs) != 0 {
		lastSeq := map[string]uint64{}
		if err := scanJournal(tx, 0, func(e journalEntry) error {
			if e.ev.Record != nil {
				if _, ok := protectedRefs[e.ev.Record.Ref]; ok {
					lastSeq[e.ev.Record.Ref] = e.seq
				}
			}
			return nil
		}); err != nil {
			return err
		}
		for _, seq := range lastSeq {
			protectedSeqs[seq] = struct{}{}
		}
	}

	var staleKeys [][]byte
	if err := scanJournal(tx, 0, func(e journalEntry) error {
		_, protected := protectedSeqs[e.seq]
		if protected {
			return nil
		}
		_, tm, _, err := unmarshalFrame(e.frame)
		if err != nil {
			return err
		}
		if tm.Before(cutoff) {
			staleKeys = append(staleKeys, sequenceKey(e.seq))
		}
		return nil
	}); err != nil {
		return err
	}
	for _, key := range staleKeys {
		if err := b.Delete(key); err != nil {
			return err
		}
	}
	return nil
}
