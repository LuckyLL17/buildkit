package worker

import (
	stderrors "errors"
	"sync"

	"github.com/containerd/containerd/v2/pkg/filters"
	"github.com/moby/buildkit/cache"
	"github.com/moby/buildkit/client"
	"github.com/pkg/errors"
)

// Controller holds worker instances.
// Currently, only local workers are supported.
type Controller struct {
	// TODO: define worker interface and support remote ones
	mu      sync.RWMutex
	workers []Worker
}

func (c *Controller) Close() error {
	c.mu.RLock()
	workers := make([]Worker, len(c.workers))
	copy(workers, c.workers)
	c.mu.RUnlock()
	var errs []error
	for _, w := range workers {
		if err := w.Close(); err != nil {
			errs = append(errs, err)
		}
	}
	return stderrors.Join(errs...)
}

// Add adds a local worker.
// The first worker becomes the default.
func (c *Controller) Add(w Worker) error {
	c.mu.Lock()
	c.workers = append(c.workers, w)
	c.mu.Unlock()
	return nil
}

// List lists workers
func (c *Controller) List(filterStrings ...string) ([]Worker, error) {
	filter, err := filters.ParseAll(filterStrings...)
	if err != nil {
		return nil, err
	}
	c.mu.RLock()
	workers := make([]Worker, 0, len(c.workers))
	for _, w := range c.workers {
		if filter.Match(adaptWorker(w)) {
			workers = append(workers, w)
		}
	}
	c.mu.RUnlock()
	return workers, nil
}

// GetDefault returns the default local worker
func (c *Controller) GetDefault() (Worker, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if len(c.workers) == 0 {
		return nil, errors.New("no default worker")
	}
	return c.workers[0], nil
}

func (c *Controller) Get(id string) (Worker, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	for _, w := range c.workers {
		if w.ID() == id {
			return w, nil
		}
	}
	return nil, errors.Errorf("worker %s not found", id)
}

// WorkerInfos returns slice of WorkerInfo.
// The first item is the default worker.
func (c *Controller) WorkerInfos() []client.WorkerInfo {
	c.mu.RLock()
	defer c.mu.RUnlock()
	out := make([]client.WorkerInfo, 0, len(c.workers))
	for _, w := range c.workers {
		out = append(out, client.WorkerInfo{
			ID:              w.ID(),
			Labels:          w.Labels(),
			Platforms:       w.Platforms(false),
			BuildkitVersion: w.BuildkitVersion(),
		})
	}
	return out
}

func (c *Controller) Infos() Infos {
	return &infosController{c: c}
}

type infosController struct {
	c *Controller
}

var _ Infos = &infosController{}

func (c *infosController) DefaultCacheManager() (cache.Manager, error) {
	w, err := c.c.GetDefault()
	if err != nil {
		return nil, err
	}
	return w.CacheManager(), nil
}

func (c *infosController) WorkerInfos() []client.WorkerInfo {
	return c.c.WorkerInfos()
}
