package helpers

import (
	"crypto/sha256"
	"encoding/binary"
	"io/fs"
	"reflect"

	"github.com/willscott/go-nfs"

	"github.com/go-git/go-billy/v5"
	"github.com/google/uuid"
	lru "github.com/hashicorp/golang-lru/v2"
)

// NewCachingHandler wraps a handler to provide a basic to/from-file handle cache.
func NewCachingHandler(h nfs.Handler, limit int) nfs.Handler {
	return NewCachingHandlerWithVerifierLimit(h, limit, limit)
}

// NewCachingHandlerWithVerifierLimit provides a basic to/from-file handle cache that can be tuned with a smaller cache of active directory listings.
func NewCachingHandlerWithVerifierLimit(h nfs.Handler, limit int, verifierLimit int) nfs.Handler {
	if limit < 2 || verifierLimit < 2 {
		nfs.Log.Warnf("Caching handler created with insufficient cache to support directory listing", "size", limit, "verifiers", verifierLimit)
	}
	cache, _ := lru.New[uuid.UUID, entry](limit)
	reverseCache := make(map[string][]uuid.UUID)
	verifiers, _ := lru.New[uint64, verifier](verifierLimit)
	return &CachingHandler{
		Handler:         h,
		activeHandles:   cache,
		reverseHandles:  reverseCache,
		activeVerifiers: verifiers,
		cacheLimit:      limit,
	}
}

// CachingHandler implements to/from handle via an LRU cache.
type CachingHandler struct {
	nfs.Handler
	activeHandles   *lru.Cache[uuid.UUID, entry]
	reverseHandles  map[string][]uuid.UUID
	activeVerifiers *lru.Cache[uint64, verifier]
	cacheLimit      int
}

type entry struct {
	f billy.Filesystem
	p []string
}

// ToHandle takes a file and represents it with an opaque handle to reference it.
// In stateless nfs (when it's serving a unix fs) this can be the device + inode
// but we can generalize with a stateful local cache of handed out IDs.
func (c *CachingHandler) ToHandle(f billy.Filesystem, path []string) []byte {
	joinedPath := f.Join(path...)

	if handle := c.searchReverseCache(f, joinedPath); handle != nil {
		return handle
	}

	id := uuid.New()

	newPath := make([]string, len(path))

	copy(newPath, path)
	evictedKey, evictedPath, ok := c.activeHandles.GetOldest()
	if evicted := c.activeHandles.Add(id, entry{f, newPath}); evicted && ok {
		rk := evictedPath.f.Join(evictedPath.p...)
		c.evictReverseCache(rk, evictedKey)
	}

	if _, ok := c.reverseHandles[joinedPath]; !ok {
		c.reverseHandles[joinedPath] = []uuid.UUID{}
	}
	c.reverseHandles[joinedPath] = append(c.reverseHandles[joinedPath], id)
	b, _ := id.MarshalBinary()

	return b
}

// FromHandle converts from an opaque handle to the file it represents
func (c *CachingHandler) FromHandle(fh []byte) (billy.Filesystem, []string, error) {
	id, err := uuid.FromBytes(fh)
	if err != nil {
		return nil, []string{}, err
	}

	if f, ok := c.activeHandles.Get(id); ok {
		for _, k := range c.activeHandles.Keys() {
			candidate, _ := c.activeHandles.Peek(k)
			if hasPrefix(f.p, candidate.p) {
				_, _ = c.activeHandles.Get(k)
			}
		}
		if ok {
			newP := make([]string, len(f.p))
			copy(newP, f.p)
			return f.f, newP, nil
		}
	}
	return nil, []string{}, &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
}

func (c *CachingHandler) searchReverseCache(f billy.Filesystem, path string) []byte {
	uuids, exists := c.reverseHandles[path]

	if !exists {
		return nil
	}

	for _, id := range uuids {
		if candidate, ok := c.activeHandles.Get(id); ok {
			if reflect.DeepEqual(candidate.f, f) {
				return id[:]
			}
		}
	}

	return nil
}

func (c *CachingHandler) evictReverseCache(path string, handle uuid.UUID) {
	uuids, exists := c.reverseHandles[path]

	if !exists {
		return
	}
	for i, u := range uuids {
		if u == handle {
			uuids = append(uuids[:i], uuids[i+1:]...)
			c.reverseHandles[path] = uuids
			return
		}
	}
}

func (c *CachingHandler) InvalidateHandle(fs billy.Filesystem, handle []byte) error {
	//Remove from cache
	id, _ := uuid.FromBytes(handle)
	entry, ok := c.activeHandles.Get(id)
	if ok {
		rk := entry.f.Join(entry.p...)
		c.evictReverseCache(rk, id)
	}
	c.activeHandles.Remove(id)
	return nil
}

// UpdateHandle updates a handle's cached path after a rename operation.
// This is critical for NFS silly rename support where files remain accessible
// via their original handle even after being renamed.
func (c *CachingHandler) UpdateHandle(fs billy.Filesystem, handle []byte, newPath []string) error {
	id, err := uuid.FromBytes(handle)
	if err != nil {
		return err
	}

	oldEntry, ok := c.activeHandles.Get(id)
	if !ok {
		return &nfs.NFSStatusError{NFSStatus: nfs.NFSStatusStale}
	}

	// Remove from old reverse cache
	oldPathJoined := oldEntry.f.Join(oldEntry.p...)
	c.evictReverseCache(oldPathJoined, id)

	// Update the entry with new path
	newPathCopy := make([]string, len(newPath))
	copy(newPathCopy, newPath)
	c.activeHandles.Add(id, entry{f: fs, p: newPathCopy})

	// Add to new reverse cache
	newPathJoined := fs.Join(newPath...)
	if _, ok := c.reverseHandles[newPathJoined]; !ok {
		c.reverseHandles[newPathJoined] = []uuid.UUID{}
	}
	c.reverseHandles[newPathJoined] = append(c.reverseHandles[newPathJoined], id)

	return nil
}

// UpdateHandlesByPath updates ALL handles matching the old path to point to the new path.
// This is used by rename operations to ensure all handles for a file are updated,
// regardless of which filesystem instance they were created with.
func (c *CachingHandler) UpdateHandlesByPath(fs billy.Filesystem, oldPath []string, newPath []string) int {
	oldPathJoined := fs.Join(oldPath...)
	uuids, exists := c.reverseHandles[oldPathJoined]
	if !exists || len(uuids) == 0 {
		return 0
	}

	// Copy the slice since we'll modify reverseHandles
	uuidsCopy := make([]uuid.UUID, len(uuids))
	copy(uuidsCopy, uuids)

	updated := 0
	newPathJoined := fs.Join(newPath...)
	newPathCopy := make([]string, len(newPath))
	copy(newPathCopy, newPath)

	for _, id := range uuidsCopy {
		oldEntry, ok := c.activeHandles.Get(id)
		if !ok {
			continue
		}

		// Remove from old reverse cache
		c.evictReverseCache(oldPathJoined, id)

		// Update the entry with new path (keep original filesystem)
		c.activeHandles.Add(id, entry{f: oldEntry.f, p: newPathCopy})

		// Add to new reverse cache
		if _, ok := c.reverseHandles[newPathJoined]; !ok {
			c.reverseHandles[newPathJoined] = []uuid.UUID{}
		}
		c.reverseHandles[newPathJoined] = append(c.reverseHandles[newPathJoined], id)
		updated++
	}

	return updated
}

// HandleLimit exports how many file handles can be safely stored by this cache.
func (c *CachingHandler) HandleLimit() int {
	return c.cacheLimit
}

func hasPrefix(path, prefix []string) bool {
	if len(prefix) > len(path) {
		return false
	}
	for i, e := range prefix {
		if path[i] != e {
			return false
		}
	}
	return true
}

type verifier struct {
	path     string
	contents []fs.FileInfo
}

func hashPathAndContents(path string, contents []fs.FileInfo) uint64 {
	//calculate a cookie-verifier.
	vHash := sha256.New()

	// Add the path to avoid collisions of directories with the same content
	vHash.Write(binary.BigEndian.AppendUint64([]byte{}, uint64(len(path))))
	vHash.Write([]byte(path))

	for _, c := range contents {
		vHash.Write([]byte(c.Name())) // Never fails according to the docs
	}

	verify := vHash.Sum(nil)[0:8]
	return binary.BigEndian.Uint64(verify)
}

func (c *CachingHandler) VerifierFor(path string, contents []fs.FileInfo) uint64 {
	id := hashPathAndContents(path, contents)
	c.activeVerifiers.Add(id, verifier{path, contents})
	return id
}

func (c *CachingHandler) DataForVerifier(path string, id uint64) []fs.FileInfo {
	if cache, ok := c.activeVerifiers.Get(id); ok {
		return cache.contents
	}
	return nil
}
