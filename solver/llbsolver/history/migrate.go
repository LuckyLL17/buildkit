package history

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/leases"
	cerrdefs "github.com/containerd/errdefs"
	controlapi "github.com/moby/buildkit/api/services/control"
	"github.com/moby/buildkit/identity"
	"github.com/moby/buildkit/util/iohelper"
	"github.com/moby/buildkit/util/leaseutil"
	digest "github.com/opencontainers/go-digest"
	ocispecs "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pkg/errors"
	bolt "go.etcd.io/bbolt"
)

// migrateV3 journals every existing record as a COMPLETE event so that all
// snapshot responses can issue cursors after the upgrade.
func (h *Queue) migrateV3() error {
	return h.opt.DB.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte(eventsBucket)); err != nil {
			return err
		}
		if b := tx.Bucket([]byte(recordsBucket)); b != nil {
			if err := b.ForEach(func(key, dt []byte) error {
				var br controlapi.BuildHistoryRecord
				if err := br.UnmarshalVT(dt); err != nil {
					return errors.Wrapf(err, "failed to unmarshal build record %s", key)
				}
				tm := br.CreatedAt.AsTime()
				if br.CompletedAt != nil {
					tm = br.CompletedAt.AsTime()
				}
				_, err := appendJournal(tx, &controlapi.BuildHistoryEvent{
					Type:   controlapi.BuildHistoryEventType_COMPLETE,
					Record: &br,
				}, tm)
				return err
			}); err != nil {
				return err
			}
		}
		vb, err := tx.CreateBucketIfNotExists([]byte(versionBucket))
		if err != nil {
			return err
		}
		return vb.Put([]byte("version"), []byte("3"))
	})
}

func (h *Queue) migrateV2() error {
	ctx := context.Background()

	if err := h.opt.DB.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(recordsBucket))
		if b == nil {
			return nil
		}
		ctx, release, err := leaseutil.WithLease(ctx, h.hLeaseManager, leases.WithID("history_migration_"+identity.NewID()), leaseutil.MakeTemporary)
		if err != nil {
			return err
		}
		defer release(context.WithoutCancel(ctx))
		return b.ForEach(func(key, dt []byte) error {
			recs, err := h.opt.LeaseManager.ListResources(ctx, leases.Lease{ID: h.leaseID(string(key))})
			if err != nil {
				if cerrdefs.IsNotFound(err) {
					return nil
				}
				return err
			}
			recs2 := make([]leases.Resource, 0, len(recs))
			for _, r := range recs {
				if r.Type == "content" {
					if ok, err := h.migrateBlobV2(ctx, r.ID, false); err != nil {
						return err
					} else if ok {
						recs2 = append(recs2, r)
					}
				} else {
					return errors.Errorf("unknown resource type %q", r.Type)
				}
			}

			l, err := h.hLeaseManager.Create(ctx, leases.WithID(h.leaseID(string(key))))
			if err != nil {
				if !errors.Is(err, cerrdefs.ErrAlreadyExists) {
					return err
				}
				l = leases.Lease{ID: string(key)}
			}

			for _, r := range recs2 {
				if err := h.hLeaseManager.AddResource(ctx, l, r); err != nil {
					return err
				}
			}

			return h.opt.LeaseManager.Delete(ctx, leases.Lease{ID: h.leaseID(string(key))})
		})
	}); err != nil {
		return err
	}

	if err := h.opt.DB.Update(func(tx *bolt.Tx) error {
		b, err := tx.CreateBucketIfNotExists([]byte(versionBucket))
		if err != nil {
			return err
		}
		return b.Put([]byte("version"), []byte("2"))
	}); err != nil {
		return err
	}

	return nil
}

func (h *Queue) blobRefs(ctx context.Context, dgst digest.Digest, detectSkipLayer bool) ([]digest.Digest, error) {
	info, err := h.opt.ContentStore.Info(ctx, dgst)
	if err != nil {
		return nil, err // allow missing blobs
	}
	var out []digest.Digest
	layers := map[digest.Digest]struct{}{}
	if detectSkipLayer {
		dt, err := content.ReadBlob(ctx, h.opt.ContentStore, ocispecs.Descriptor{
			Digest: dgst,
		})
		if err != nil {
			return nil, err
		}
		var mfst ocispecs.Manifest
		if err := json.Unmarshal(dt, &mfst); err != nil {
			return nil, err
		}
		for _, l := range mfst.Layers {
			layers[l.Digest] = struct{}{}
		}
	}
	for k, v := range info.Labels {
		if !strings.HasPrefix(k, "containerd.io/gc.ref.content.") {
			continue
		}
		dgst, err := digest.Parse(v)
		if err != nil {
			continue
		}
		if _, ok := layers[dgst]; ok {
			continue
		}
		out = append(out, dgst)
	}
	return out, nil
}

func (h *Queue) migrateBlobV2(ctx context.Context, id string, detectSkipLayers bool) (bool, error) {
	dgst, err := digest.Parse(id)
	if err != nil {
		return false, err
	}

	refs, _ := h.blobRefs(ctx, dgst, detectSkipLayers) // allow missing blobs
	labels := map[string]string{}
	for i, r := range refs {
		labels["containerd.io/gc.ref.content."+strconv.Itoa(i)] = r.String()
	}

	w, err := content.OpenWriter(ctx, h.hContentStore, content.WithDescriptor(ocispecs.Descriptor{
		Digest: dgst,
	}), content.WithRef("history-migrate-"+id))
	if err != nil {
		if cerrdefs.IsAlreadyExists(err) {
			return true, nil
		}
		return false, err
	}
	defer w.Close()
	ra, err := h.opt.ContentStore.ReaderAt(ctx, ocispecs.Descriptor{
		Digest: dgst,
	})
	if err != nil {
		return false, nil // allow skipping
	}
	defer ra.Close()
	if err := content.Copy(ctx, w, iohelper.ReadCloser(ra), 0, dgst, content.WithLabels(labels)); err != nil {
		return false, err
	}

	for _, refs := range refs {
		h.migrateBlobV2(ctx, refs.String(), detectSkipLayers) // allow missing blobs
	}

	return true, nil
}
