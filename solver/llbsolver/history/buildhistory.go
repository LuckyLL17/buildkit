package history

import (
	"bufio"
	"context"
	"encoding/binary"
	"io"
	"os"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/leases"
	cerrdefs "github.com/containerd/errdefs"
	controlapi "github.com/moby/buildkit/api/services/control"
	"github.com/moby/buildkit/client"
	"github.com/moby/buildkit/cmd/buildkitd/config"
	"github.com/moby/buildkit/identity"
	containerdsnapshot "github.com/moby/buildkit/snapshot/containerd"
	"github.com/moby/buildkit/util/bklog"
	"github.com/moby/buildkit/util/db"
	"github.com/moby/buildkit/util/grpcerrors"
	"github.com/moby/buildkit/util/iohelper"
	"github.com/moby/buildkit/util/leaseutil"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
	spb "google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	recordsBucket = "_records"
	versionBucket = "_version"
)

type QueueOpt struct {
	DB             db.Transactor
	LeaseManager   *leaseutil.Manager
	ContentStore   *containerdsnapshot.Store
	CleanConfig    *config.HistoryConfig
	GarbageCollect func(context.Context) error
	GracefulStop   <-chan struct{}
}

type Queue struct {
	// mu protects active, refs and deleted maps
	mu            sync.Mutex
	initOnce      sync.Once
	opt           QueueOpt
	ps            *pubsub[*controlapi.BuildHistoryEvent]
	active        map[string]*controlapi.BuildHistoryRecord
	finalizers    map[string]*finalizer
	refs          map[string]int
	deleted       map[string]struct{}
	hContentStore *containerdsnapshot.Store
	hLeaseManager *leaseutil.Manager
}

// finalizer controls completion of saving traces for a
// record and making it immutable
type finalizer struct {
	trigger func()
	done    chan struct{}
}

type StatusImportResult struct {
	Descriptor        ocispecs.Descriptor
	NumCachedSteps    int
	NumCompletedSteps int
	NumTotalSteps     int
	NumWarnings       int
}

const (
	statusFlagCached uint8 = 1 << iota
	statusFlagCompleted
)

// StatusSummary contains the build-step counts used by history records and metrics.
type StatusSummary struct {
	NumCachedSteps    int
	NumCompletedSteps int
	NumTotalSteps     int
	NumWarnings       int
	vertices          map[digest.Digest]uint8
}

// Update incorporates one solve-status update into the summary.
func (s *StatusSummary) Update(st *client.SolveStatus) {
	if s.vertices == nil {
		s.vertices = map[digest.Digest]uint8{}
	}
	s.NumWarnings += len(st.Warnings)
	for _, vertex := range st.Vertexes {
		state, ok := s.vertices[vertex.Digest]
		if !ok {
			s.NumTotalSteps++
		}
		if vertex.Cached && state&statusFlagCached == 0 {
			s.NumCachedSteps++
			state |= statusFlagCached
		}
		if vertex.Completed != nil && state&statusFlagCompleted == 0 {
			s.NumCompletedSteps++
			state |= statusFlagCompleted
		}
		s.vertices[vertex.Digest] = state
	}
}

// Enabled reports whether new build history events and records are captured.
func (h *Queue) Enabled() bool {
	return h.opt.CleanConfig == nil || h.opt.CleanConfig.MaxEntries == nil || *h.opt.CleanConfig.MaxEntries != 0
}

func NewQueue(opt QueueOpt) (*Queue, error) {
	if opt.CleanConfig == nil {
		maxEntries := int64(50)
		opt.CleanConfig = &config.HistoryConfig{
			MaxAge:     config.Duration{Duration: 48 * time.Hour},
			MaxEntries: &maxEntries,
		}
	}
	h := &Queue{
		opt: opt,
		ps: &pubsub[*controlapi.BuildHistoryEvent]{
			m: map[*channel[*controlapi.BuildHistoryEvent]]struct{}{},
		},
		active:     map[string]*controlapi.BuildHistoryRecord{},
		refs:       map[string]int{},
		deleted:    map[string]struct{}{},
		finalizers: map[string]*finalizer{},
	}

	ns := h.opt.ContentStore.Namespace()
	// double check invalid configuration
	ns2 := h.opt.LeaseManager.Namespace()
	if ns != ns2 {
		return nil, errors.Errorf("invalid configuration: content store namespace %q does not match lease manager namespace %q", ns, ns2)
	}
	h.hContentStore = h.opt.ContentStore.WithNamespace(ns + "_history")
	h.hLeaseManager = h.opt.LeaseManager.WithNamespace(ns + "_history")

	// v2 migration: all records need to be on isolated containerd ns from rest of buildkit
	version := int64(0)
	if err := h.opt.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(versionBucket))
		if b != nil {
			if v := b.Get([]byte("version")); v != nil {
				version, _ = strconv.ParseInt(string(v), 10, 64)
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	if version < 2 {
		if err := h.migrateV2(); err != nil {
			return nil, err
		}
	}
	// v3 migration: journal all existing records so every record has a cursor
	if version < 3 {
		if err := h.migrateV3(); err != nil {
			return nil, err
		}
	}

	go func() {
		h.clearOrphans()
		for {
			h.gc()
			time.Sleep(120 * time.Second)
		}
	}()

	go func() {
		<-h.opt.GracefulStop
		h.mu.Lock()
		defer h.mu.Unlock()
		// if active builds then close will happen in finalizer
		if len(h.finalizers) == 0 && len(h.active) == 0 {
			go h.ps.Close()
		}
	}()

	return h, nil
}

// maxEntries returns the retention floor used by GC. An unset MaxEntries in a
// present history section and an explicit zero both return zero; Enabled keeps
// them distinct because only an explicit zero disables recording.
func (h *Queue) maxEntries() int64 {
	if h.opt.CleanConfig == nil {
		return 50
	}
	if h.opt.CleanConfig.MaxEntries == nil {
		return 0
	}
	return *h.opt.CleanConfig.MaxEntries
}

func (h *Queue) gc() error {
	var records []*controlapi.BuildHistoryRecord

	if err := h.opt.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(recordsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(key, dt []byte) error {
			var br controlapi.BuildHistoryRecord
			if err := br.UnmarshalVT(dt); err != nil {
				return errors.Wrapf(err, "failed to unmarshal build record %s", key)
			}
			if br.Pinned {
				return nil
			}
			records = append(records, &br)
			return nil
		})
	}); err != nil {
		return err
	}

	// in order for record to get deleted by gc it exceed both maxentries and maxage criteria
	maxEntries := h.maxEntries()

	// sort array by newest records first
	slices.SortFunc(records, func(a, b *controlapi.BuildHistoryRecord) int {
		return -a.CompletedAt.AsTime().Compare(b.CompletedAt.AsTime())
	})

	h.mu.Lock()
	defer h.mu.Unlock()

	now := time.Now()
	if int64(len(records)) > maxEntries {
		for _, r := range records[maxEntries:] {
			if now.Add(-h.opt.CleanConfig.MaxAge.Duration).After(r.CompletedAt.AsTime()) {
				if _, err := h.delete(r.Ref); err != nil {
					return err
				}
			}
		}
	}

	// prune journal frames that are older than the maxage retention, keeping
	// one anchor frame per ref with a live record or active build
	protectedRefs := map[string]struct{}{}
	if err := h.opt.DB.View(func(tx *bolt.Tx) error {
		if b := tx.Bucket([]byte(recordsBucket)); b != nil {
			c := b.Cursor()
			for k, _ := c.First(); k != nil; k, _ = c.Next() {
				protectedRefs[string(k)] = struct{}{}
			}
		}
		return nil
	}); err != nil {
		return err
	}
	for ref := range h.active {
		protectedRefs[ref] = struct{}{}
	}
	return h.opt.DB.Update(func(tx *bolt.Tx) error {
		return pruneJournal(tx, protectedRefs, now.Add(-h.opt.CleanConfig.MaxAge.Duration))
	})
}

func (h *Queue) clearOrphans() error {
	ctx := context.Background()
	var records []*controlapi.BuildHistoryRecord

	if err := h.opt.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(recordsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(key, dt []byte) error {
			var br controlapi.BuildHistoryRecord
			if err := proto.Unmarshal(dt, &br); err != nil {
				return errors.Wrapf(err, "failed to unmarshal build record %s", key)
			}
			recs, err := h.hLeaseManager.ListResources(ctx, leases.Lease{ID: h.leaseID(string(key))})
			if (err != nil && cerrdefs.IsNotFound(err)) || len(recs) == 0 {
				records = append(records, &br)
			}
			return nil
		})
	}); err != nil {
		return err
	}

	if len(records) == 0 {
		return nil
	}

	h.mu.Lock()
	defer h.mu.Unlock()

	for _, r := range records {
		bklog.G(ctx).Warnf("deleting build record %s due to missing blobs", r.Ref)
		if _, err := h.delete(r.Ref); err != nil {
			return err
		}
	}

	return nil
}

func (h *Queue) delete(ref string) (bool, error) {
	if _, ok := h.refs[ref]; ok {
		h.deleted[ref] = struct{}{}
		return false, nil
	}
	delete(h.deleted, ref)

	var ev *controlapi.BuildHistoryEvent
	if err := h.opt.DB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(recordsBucket))
		if b == nil {
			return errors.Wrapf(os.ErrNotExist, "failed to retrieve bucket %s", recordsBucket)
		}
		err1 := b.Delete([]byte(ref))
		err2 := h.hLeaseManager.Delete(context.TODO(), leases.Lease{ID: h.leaseID(ref)})
		if err1 != nil {
			return err1
		}
		if err2 != nil {
			return err2
		}
		var err error
		ev, err = appendJournal(tx, &controlapi.BuildHistoryEvent{
			Type:   controlapi.BuildHistoryEventType_DELETED,
			Record: &controlapi.BuildHistoryRecord{Ref: ref},
		}, time.Now())
		return err
	}); err != nil {
		return false, err
	}
	h.ps.Send(ev)
	return true, nil
}

func (h *Queue) init() error {
	var err error
	h.initOnce.Do(func() {
		err = h.opt.DB.Update(func(tx *bolt.Tx) error {
			if _, err := tx.CreateBucketIfNotExists([]byte(recordsBucket)); err != nil {
				return err
			}
			_, err := tx.CreateBucketIfNotExists([]byte(eventsBucket))
			return err
		})
	})
	return err
}

func (h *Queue) leaseID(id string) string {
	return "ref_" + id
}

func (h *Queue) addResource(ctx context.Context, l leases.Lease, desc *controlapi.Descriptor, detectSkipLayers bool) error {
	if desc == nil {
		return nil
	}
	if _, err := h.hContentStore.Info(ctx, digest.Digest(desc.Digest)); err != nil {
		if cerrdefs.IsNotFound(err) {
			lr, ctx, err := leaseutil.NewLease(ctx, h.hLeaseManager, leases.WithID("history_migration_"+identity.NewID()), leaseutil.MakeTemporary)
			if err != nil {
				return err
			}
			defer lr.Discard(ctx)
			ok, err := h.migrateBlobV2(ctx, desc.Digest, detectSkipLayers)
			if err != nil {
				return err
			}
			if !ok {
				return errors.Errorf("unknown blob %s in history", desc.Digest)
			}
		}
	}
	return h.hLeaseManager.AddResource(ctx, l, leases.Resource{
		ID:   desc.Digest,
		Type: "content",
	})
}

func (h *Queue) UpdateRef(ctx context.Context, ref string, upt func(r *controlapi.BuildHistoryRecord) error) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	var br controlapi.BuildHistoryRecord
	if err := h.opt.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(recordsBucket))
		if b == nil {
			return errors.Wrapf(os.ErrNotExist, "failed to retrieve bucket %s", recordsBucket)
		}
		dt := b.Get([]byte(ref))
		if dt == nil {
			return errors.Wrapf(os.ErrNotExist, "failed to retrieve ref %s", ref)
		}

		if err := br.UnmarshalVT(dt); err != nil {
			return errors.Wrapf(err, "failed to unmarshal build record %s", ref)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := upt(&br); err != nil {
		return err
	}
	br.Generation++

	if br.Ref != ref {
		return errors.New("invalid ref change")
	}

	ev := &controlapi.BuildHistoryEvent{
		Type:   controlapi.BuildHistoryEventType_COMPLETE,
		Record: &br,
	}
	if err := h.opt.DB.Update(func(tx *bolt.Tx) error {
		if err := h.updateRecord(ctx, tx, &br); err != nil {
			return err
		}
		var err error
		ev, err = appendJournal(tx, ev, br.CompletedAt.AsTime())
		return err
	}); err != nil {
		return err
	}
	h.ps.Send(ev)
	return nil
}

func (h *Queue) Status(ctx context.Context, ref string, st chan<- *client.SolveStatus) error {
	h.init()
	var br controlapi.BuildHistoryRecord
	if err := h.opt.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(recordsBucket))
		if b == nil {
			return errors.Wrapf(os.ErrNotExist, "failed to retrieve bucket %s", recordsBucket)
		}
		dt := b.Get([]byte(ref))
		if dt == nil {
			return errors.Wrapf(os.ErrNotExist, "failed to retrieve ref %s", ref)
		}

		if err := br.UnmarshalVT(dt); err != nil {
			return errors.Wrapf(err, "failed to unmarshal build record %s", ref)
		}
		return nil
	}); err != nil {
		return err
	}

	if br.Logs == nil {
		return nil
	}

	ra, err := h.hContentStore.ReaderAt(ctx, ocispecs.Descriptor{
		Digest:    digest.Digest(br.Logs.Digest),
		Size:      br.Logs.Size,
		MediaType: br.Logs.MediaType,
	})
	if err != nil {
		return err
	}

	rc := iohelper.ReadCloser(ra)
	defer rc.Close()

	brdr := bufio.NewReader(rc)

	buf := make([]byte, 32*1024)

	for {
		_, err := io.ReadAtLeast(brdr, buf[:4], 4)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		sz := binary.LittleEndian.Uint32(buf[:4])
		if sz > uint32(len(buf)) {
			buf = make([]byte, sz)
		}
		_, err = io.ReadAtLeast(brdr, buf[:sz], int(sz))
		if err != nil {
			return err
		}
		var sr controlapi.StatusResponse
		if err := sr.UnmarshalVT(buf[:sz]); err != nil {
			return err
		}
		st <- client.NewSolveStatus(&sr)
	}

	return nil
}

func (h *Queue) updateRecord(ctx context.Context, tx *bolt.Tx, rec *controlapi.BuildHistoryRecord) (err error) {
	b := tx.Bucket([]byte(recordsBucket))
	if b == nil {
		return nil
	}
	dt, err := rec.MarshalVT()
	if err != nil {
		return err
	}

	l, err := h.hLeaseManager.Create(ctx, leases.WithID(h.leaseID(rec.Ref)))
	created := true
	if err != nil {
		if !errors.Is(err, cerrdefs.ErrAlreadyExists) {
			return err
		}
		l = leases.Lease{ID: h.leaseID(rec.Ref)}
		created = false
	}

	defer func() {
		if err != nil && created {
			h.hLeaseManager.Delete(context.WithoutCancel(ctx), l)
		}
	}()

	if err := h.addResource(ctx, l, rec.Logs, false); err != nil {
		return err
	}
	if err := h.addResource(ctx, l, rec.Trace, false); err != nil {
		return err
	}
	if err := h.addResource(ctx, l, rec.ExternalError, false); err != nil {
		return err
	}
	if rec.Result != nil {
		if err := h.addResource(ctx, l, rec.Result.ResultDeprecated, true); err != nil {
			return err
		}
		for _, res := range rec.Result.Results {
			if err := h.addResource(ctx, l, res, true); err != nil {
				return err
			}
		}
		for _, att := range rec.Result.Attestations {
			if err := h.addResource(ctx, l, att, false); err != nil {
				return err
			}
		}
	}
	for _, r := range rec.Results {
		if err := h.addResource(ctx, l, r.ResultDeprecated, true); err != nil {
			return err
		}
		for _, res := range r.Results {
			if err := h.addResource(ctx, l, res, true); err != nil {
				return err
			}
		}
		for _, att := range r.Attestations {
			if err := h.addResource(ctx, l, att, false); err != nil {
				return err
			}
		}
	}

	return b.Put([]byte(rec.Ref), dt)
}

func (h *Queue) AcquireFinalizer(ref string) (<-chan struct{}, func()) {
	h.mu.Lock()
	defer h.mu.Unlock()
	trigger := make(chan struct{})
	f := &finalizer{
		trigger: sync.OnceFunc(func() {
			close(trigger)
		}),
		done: make(chan struct{}),
	}
	h.finalizers[ref] = f
	go func() {
		<-f.done
		h.mu.Lock()
		delete(h.finalizers, ref)
		// if gracefulstop then release listeners after finalize
		if len(h.finalizers) == 0 {
			select {
			case <-h.opt.GracefulStop:
				go h.ps.Close()
			default:
			}
		}
		h.mu.Unlock()
	}()
	return trigger, sync.OnceFunc(func() {
		close(f.done)
	})
}

func (h *Queue) Finalize(ctx context.Context, ref string) error {
	h.mu.Lock()
	f, ok := h.finalizers[ref]
	h.mu.Unlock()
	if !ok {
		return nil
	}
	f.trigger()
	<-f.done
	return nil
}

func (h *Queue) Update(ctx context.Context, e *controlapi.BuildHistoryEvent) error {
	h.init()
	h.mu.Lock()
	defer h.mu.Unlock()

	e = e.CloneVT()

	if e.Type == controlapi.BuildHistoryEventType_STARTED {
		rec := e.Record
		h.active[rec.Ref] = rec
		if err := h.opt.DB.Update(func(tx *bolt.Tx) error {
			ev, err := appendJournal(tx, e, rec.CreatedAt.AsTime())
			if err != nil {
				return err
			}
			e = ev
			return nil
		}); err != nil {
			delete(h.active, rec.Ref)
			return err
		}
		h.ps.Send(e)
	}

	if e.Type == controlapi.BuildHistoryEventType_COMPLETE {
		rec := e.Record
		delete(h.active, rec.Ref)
		if err := h.opt.DB.Update(func(tx *bolt.Tx) error {
			if err := h.updateRecord(ctx, tx, rec); err != nil {
				return err
			}
			ev, err := appendJournal(tx, e, rec.CompletedAt.AsTime())
			if err != nil {
				return err
			}
			e = ev
			return nil
		}); err != nil {
			return err
		}
		h.ps.Send(e)
	}
	return nil
}

func (h *Queue) Delete(ctx context.Context, ref string) error {
	h.mu.Lock()
	defer h.mu.Unlock()

	v, err := h.delete(ref)
	if err != nil {
		return err
	}
	if v {
		return h.opt.GarbageCollect(ctx)
	}
	return nil
}

func (h *Queue) OpenBlobWriter(ctx context.Context, mt string) (_ *Writer, err error) {
	l, err := h.hLeaseManager.Create(ctx, leases.WithRandomID(), leases.WithExpiration(5*time.Minute), leaseutil.MakeTemporary)
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			h.hLeaseManager.Delete(context.WithoutCancel(ctx), l)
		}
	}()

	ctx = leases.WithLease(ctx, l.ID)

	w, err := content.OpenWriter(ctx, h.hContentStore, content.WithRef("history-"+h.leaseID(l.ID)))
	if err != nil {
		return nil, err
	}

	return &Writer{
		mt:    mt,
		lm:    h.hLeaseManager,
		l:     l,
		w:     w,
		dgstr: digest.Canonical.Digester(),
	}, nil
}

type Writer struct {
	mt string
	w  content.Writer
	lm leases.Manager
	l  leases.Lease

	dgstr digest.Digester
	sz    int
}

func (w *Writer) Write(p []byte) (int, error) {
	if _, err := w.dgstr.Hash().Write(p); err != nil {
		return 0, err
	}
	w.sz += len(p)
	return w.w.Write(p)
}

func (w *Writer) Discard() {
	w.w.Close()
	w.lm.Delete(context.TODO(), w.l)
}

func (w *Writer) Commit(ctx context.Context) (*ocispecs.Descriptor, func(), error) {
	dgst := w.dgstr.Digest()
	sz := int64(w.sz)
	if err := w.w.Commit(leases.WithLease(ctx, w.l.ID), int64(w.sz), dgst); err != nil {
		if !cerrdefs.IsAlreadyExists(err) {
			w.Discard()
			return nil, nil, err
		}
	}
	return &ocispecs.Descriptor{
			MediaType: w.mt,
			Digest:    dgst,
			Size:      sz,
		},
		func() {
			w.lm.Delete(context.TODO(), w.l)
		}, nil
}

// BuildErrorStatus converts a build error to the status stored in history and
// consumed by build metrics.
func BuildErrorStatus(ctx context.Context, err error) *spb.Status {
	if err == nil {
		return nil
	}
	st, ok := grpcerrors.AsGRPCStatus(grpcerrors.ToGRPC(ctx, err))
	if !ok {
		st = status.New(codes.Unknown, err.Error())
	}
	return st.Proto()
}

func (h *Queue) ImportError(ctx context.Context, err error) (_ *spb.Status, _ *controlapi.Descriptor, _ func(), retErr error) {
	stpb := BuildErrorStatus(ctx, err)
	dt, err := proto.Marshal(stpb)
	if err != nil {
		return nil, nil, nil, err
	}

	w, err := h.OpenBlobWriter(ctx, "application/vnd.googleapis.google.rpc.status+proto")
	if err != nil {
		return nil, nil, nil, err
	}

	defer func() {
		if retErr != nil {
			w.Discard()
		}
	}()

	if _, err := w.Write(dt); err != nil {
		return nil, nil, nil, err
	}

	desc, release, err := w.Commit(ctx)
	if err != nil {
		return nil, nil, nil, err
	}

	// clear details part of the error that are saved to main record
	stpb.Details = nil

	return stpb, &controlapi.Descriptor{
		Digest:    string(desc.Digest),
		Size:      desc.Size,
		MediaType: desc.MediaType,
	}, release, nil
}

func (h *Queue) ImportStatus(ctx context.Context, ch chan *client.SolveStatus) (_ *StatusImportResult, _ func(), err error) {
	defer func() {
		if ch == nil {
			return
		}
		for range ch {
		}
	}()

	w, err := h.OpenBlobWriter(ctx, "application/vnd.buildkit.status.v0")
	if err != nil {
		return nil, nil, err
	}

	bufW := bufio.NewWriter(w)

	defer func() {
		if err != nil {
			w.Discard()
		}
	}()

	var summary StatusSummary

	buf := make([]byte, 32*1024)
	for st := range ch {
		summary.Update(st)

		hdr := make([]byte, 4)
		for _, pst := range st.Marshal() {
			sz := pst.SizeVT()
			if len(buf) < sz {
				buf = make([]byte, sz)
			}
			n, err := pst.MarshalToVT(buf)
			if err != nil {
				return nil, nil, err
			}
			binary.LittleEndian.PutUint32(hdr, uint32(n))
			if _, err := bufW.Write(hdr); err != nil {
				return nil, nil, err
			}
			if _, err := bufW.Write(buf[:n]); err != nil {
				return nil, nil, err
			}
		}
	}
	if err := bufW.Flush(); err != nil {
		return nil, nil, err
	}
	desc, release, err := w.Commit(ctx)
	if err != nil {
		return nil, nil, err
	}

	return &StatusImportResult{
		Descriptor:        *desc,
		NumCachedSteps:    summary.NumCachedSteps,
		NumCompletedSteps: summary.NumCompletedSteps,
		NumTotalSteps:     summary.NumTotalSteps,
		NumWarnings:       summary.NumWarnings,
	}, release, nil
}

func (h *Queue) Listen(ctx context.Context, req *controlapi.BuildHistoryRequest, f func(*controlapi.BuildHistoryEvent) error) error {
	h.init()

	if req.Cursor != "" {
		anchorSeq, anchorDigest, err := decodeCursor(req.Cursor)
		if err != nil {
			return status.Error(codes.InvalidArgument, err.Error())
		}
		return h.listenIncremental(ctx, req, f, anchorSeq, anchorDigest)
	}
	return h.listenSnapshot(ctx, req, f)
}

// trackRef registers a ref-counted listener for req.Ref. The returned cleanup
// function must be invoked while h.mu is held.
func (h *Queue) trackRef(req *controlapi.BuildHistoryRequest) (func(), error) {
	if req.Ref == "" {
		return func() {}, nil
	}
	if _, ok := h.deleted[req.Ref]; ok {
		return nil, errors.Wrapf(os.ErrNotExist, "ref %s is deleted", req.Ref)
	}
	h.refs[req.Ref]++
	return func() {
		h.refs[req.Ref]--
		if _, ok := h.deleted[req.Ref]; !ok || h.refs[req.Ref] != 0 {
			return
		}
		delete(h.refs, req.Ref)
		h.delete(req.Ref)
	}, nil
}

type eventAnchor struct {
	seq  uint64
	dgst digest.Digest
}

func (h *Queue) listenSnapshot(ctx context.Context, req *controlapi.BuildHistoryRequest, f func(*controlapi.BuildHistoryEvent) error) error {
	h.mu.Lock()
	sub := h.ps.Subscribe()
	defer sub.close()

	releaseRef, err := h.trackRef(req)
	if err != nil {
		h.mu.Unlock()
		return err
	}
	defer func() {
		h.mu.Lock()
		releaseRef()
		h.mu.Unlock()
	}()

	// make a copy of events for active builds so we don't keep a lock during grpc send
	actives := make([]*controlapi.BuildHistoryEvent, 0, len(h.active))
	for _, e := range h.active {
		if req.Ref != "" && e.Ref != req.Ref {
			continue
		}
		if _, ok := h.deleted[e.Ref]; ok {
			continue
		}
		actives = append(actives, &controlapi.BuildHistoryEvent{
			Type:   controlapi.BuildHistoryEventType_STARTED,
			Record: e,
		})
	}
	h.mu.Unlock()

	started := map[string]eventAnchor{}
	completed := map[string]eventAnchor{}
	var events []*controlapi.BuildHistoryEvent

	if err := h.opt.DB.View(func(tx *bolt.Tx) error {
		if err := scanJournal(tx, 0, func(e journalEntry) error {
			if e.ev.Record == nil {
				return nil
			}
			a := eventAnchor{seq: e.seq, dgst: digest.FromBytes(e.frame)}
			switch e.ev.Type {
			case controlapi.BuildHistoryEventType_STARTED:
				started[e.ev.Record.Ref] = a
			case controlapi.BuildHistoryEventType_COMPLETE:
				completed[e.ev.Record.Ref] = a
			}
			return nil
		}); err != nil {
			return err
		}
		if req.ActiveOnly {
			return nil
		}
		b := tx.Bucket([]byte(recordsBucket))
		if b == nil {
			return nil
		}
		return b.ForEach(func(key, dt []byte) error {
			if req.Ref != "" && req.Ref != string(key) {
				return nil
			}
			var br controlapi.BuildHistoryRecord
			if err := br.UnmarshalVT(dt); err != nil {
				return errors.Wrapf(err, "failed to unmarshal build record %s", key)
			}
			events = append(events, &controlapi.BuildHistoryEvent{
				Record: &br,
				Type:   controlapi.BuildHistoryEventType_COMPLETE,
			})
			return nil
		})
	}); err != nil {
		return err
	}

	// filter out records that have been marked for deletion
	h.mu.Lock()
	for i, e := range events {
		if _, ok := h.deleted[e.Record.Ref]; ok {
			events[i] = nil
		}
	}
	h.mu.Unlock()

	events, err = filterHistoryEvents(events, req.Filter, req.Limit)
	if err != nil {
		return err
	}

	var lastSeq uint64
	send := func(e *controlapi.BuildHistoryEvent, a eventAnchor) error {
		if a.seq != 0 {
			e.Cursor = encodeCursor(a.seq, a.dgst)
			if a.seq > lastSeq {
				lastSeq = a.seq
			}
		}
		return f(e)
	}

	for _, e := range actives {
		if err := send(e, started[e.Record.Ref]); err != nil {
			return err
		}
	}
	for _, e := range events {
		if e == nil || e.Record == nil {
			continue
		}
		if err := send(e, completed[e.Record.Ref]); err != nil {
			return err
		}
	}

	if req.EarlyExit {
		return nil
	}
	return h.liveLoop(ctx, req, sub, lastSeq, f)
}

func (h *Queue) listenIncremental(ctx context.Context, req *controlapi.BuildHistoryRequest, f func(*controlapi.BuildHistoryEvent) error, anchorSeq uint64, anchorDigest digest.Digest) error {
	h.mu.Lock()
	sub := h.ps.Subscribe()
	defer sub.close()

	releaseRef, err := h.trackRef(req)
	if err != nil {
		h.mu.Unlock()
		return err
	}
	defer func() {
		h.mu.Lock()
		releaseRef()
		h.mu.Unlock()
	}()
	h.mu.Unlock()

	var entries []journalEntry
	if err := h.opt.DB.View(func(tx *bolt.Tx) error {
		if err := validateCursorAnchor(tx, anchorSeq, anchorDigest); err != nil {
			return err
		}
		return scanJournal(tx, anchorSeq, func(e journalEntry) error {
			if req.Ref != "" && (e.ev.Record == nil || req.Ref != e.ev.Record.Ref) {
				return nil
			}
			if req.ActiveOnly && e.ev.Type == controlapi.BuildHistoryEventType_COMPLETE {
				return nil
			}
			entries = append(entries, e)
			return nil
		})
	}); err != nil {
		return err
	}

	fns, err := parseFilters(req.Filter)
	if err != nil {
		return status.Error(codes.InvalidArgument, err.Error())
	}
	filtered := make([]journalEntry, 0, len(entries))
	for _, e := range entries {
		if fns == nil || matchHistoryFilter(fns, e.ev) {
			filtered = append(filtered, e)
		}
	}

	if req.Limit < 0 {
		return status.Error(codes.InvalidArgument, errors.Errorf("invalid limit %d", req.Limit).Error())
	}
	if req.Limit > 0 && int32(len(filtered)) > req.Limit {
		filtered = filtered[:req.Limit]
	}

	lastSeq := anchorSeq
	for _, e := range filtered {
		ev := e.ev.CloneVT()
		dgst := digest.FromBytes(e.frame)
		ev.Cursor = encodeCursor(e.seq, dgst)
		lastSeq = e.seq
		if err := f(ev); err != nil {
			return err
		}
	}

	if req.EarlyExit {
		return nil
	}
	return h.liveLoop(ctx, req, sub, lastSeq, f)
}

func (h *Queue) liveLoop(ctx context.Context, req *controlapi.BuildHistoryRequest, sub *channel[*controlapi.BuildHistoryEvent], lastSeq uint64, f func(*controlapi.BuildHistoryEvent) error) error {
	for {
		select {
		case <-ctx.Done():
			return context.Cause(ctx)
		case e := <-sub.ch:
			if seq, _, err := decodeCursor(e.Cursor); err == nil {
				if seq <= lastSeq {
					continue
				}
				lastSeq = seq
			}
			if req.Ref != "" && req.Ref != e.Record.Ref {
				continue
			}
			if err := f(e); err != nil {
				return err
			}
		case <-sub.done:
			return nil
		}
	}
}
