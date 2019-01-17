/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.master.metastore.caching;

import alluxio.PropertyKey;
import alluxio.conf.InstancedConfiguration;
import alluxio.master.file.meta.Edge;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeLockManager;
import alluxio.master.file.meta.InodeTree.LockMode;
import alluxio.master.file.meta.MutableInode;
import alluxio.master.metastore.InodeStore;
import alluxio.resource.LockResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

/**
 * An inode store which caches inode tree metadata and delegates to another inode store for cache
 * misses.
 */
public final class CachingInodeStore implements InodeStore {
  private final InodeStore mBackingStore;
  private final InodeLockManager mLockManager;

  // Cache recently-accessed inodes.
  private final InodeCache mInodeCache;

  // Cache recently-accessed inode tree edges.
  private final EdgeCache mEdgeCache;

  private final ListingCache mListingCache;

  /**
   * @param backingStore the backing inode store
   * @param conf configuration
   */
  public CachingInodeStore(InodeStore backingStore, InodeLockManager lockManager, InstancedConfiguration conf) {
    mBackingStore = backingStore;
    mLockManager = lockManager;
    int maxSize = conf.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_MAX_SIZE);
    int highWaterMark =
        Math.round(maxSize * conf.getFloat(PropertyKey.MASTER_METASTORE_INODE_CACHE_HIGH_WATER_MARK_RATIO));
    int lowWaterMark =
        Math.round(maxSize * conf.getFloat(PropertyKey.MASTER_METASTORE_INODE_CACHE_LOW_WATER_MARK_RATIO));

    mInodeCache = new InodeCache(maxSize, highWaterMark, lowWaterMark);
    mEdgeCache = new EdgeCache(maxSize, highWaterMark, lowWaterMark);
    mListingCache = new ListingCache();
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id) {
    return mInodeCache.get(id);
  }

  @Override
  public void remove(Long inodeId) {
    mInodeCache.remove(inodeId);
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    mInodeCache.put(inode.getId(), inode);
  }

  @Override
  public void clear() {
    mInodeCache.clear();
    mEdgeCache.clear();
    mBackingStore.clear();
  }

  @Override
  public void addChild(long parentId, String childName, Long childId) {
    System.out.printf("Add %s->%s(%s)%n", parentId, childName, childId);
    mEdgeCache.put(new Edge(parentId, childName), childId);
  }

  @Override
  public void removeChild(long parentId, String name) {
    System.out.printf("Remove %s->%s%n", parentId, name);
    mEdgeCache.remove(new Edge(parentId, name));
  }

  @Override
  public long estimateSize() {
    return mBackingStore.estimateSize();
  }

  @Override
  public Iterable<Long> getChildIds(Long inodeId) {
    return () -> mListingCache.getChildIds(inodeId).iterator();
  }

  @Override
  public Iterable<? extends Inode> getChildren(Long inodeId) {
    Iterator<Long> childIterator = mListingCache.getChildIds(inodeId).iterator();
    return () -> new Iterator<Inode>() {
      private Inode mNext = null;

      @Override
      public boolean hasNext() {
        advance();
        return mNext != null;
      }

      @Override
      public Inode next() {
        if (!hasNext()) {
          throw new NoSuchElementException("No more children available");
        }
        Inode next = mNext;
        mNext = null;
        return next;
      }

      private void advance() {
        while (mNext == null && childIterator.hasNext()) {
          Long nextId = childIterator.next();
          Optional<Inode> nextInode = get(nextId);
          if (nextInode.isPresent()) {
            mNext = nextInode.get();
          }
        }
      }
    };
  }

  @Override
  public Optional<Long> getChildId(Long inodeId, String name) {
    return mEdgeCache.get(new Edge(inodeId, name));
  }

  @Override
  public Optional<Inode> getChild(Long inodeId, String name) {
    return mEdgeCache.get(new Edge(inodeId, name)).flatMap(this::get);
  }

  @Override
  public boolean hasChildren(InodeDirectoryView inode) {
    Optional<Collection<Long>> cached = mListingCache.getCachedChildIds(inode.getId());
    if (cached.isPresent()) {
      return cached.get().isEmpty();
    }
    return !mEdgeCache.getChildIds(inode.getId()).isEmpty() || mBackingStore.hasChildren(inode);
  }

  private class InodeCache extends Cache<Long, MutableInode<?>> {
    public InodeCache(int maxSize, int highWaterMark, int lowWaterMark) {
      super(maxSize, highWaterMark, lowWaterMark);
    }

    @Override
    protected Optional<MutableInode<?>> load(Long id) {
      return mBackingStore.getMutable(id);
    }

    @Override
    protected boolean flush(Entry entry) {
      Optional<LockResource> lockOpt = mLockManager.tryLockInode(entry.mKey, LockMode.WRITE);
      if (!lockOpt.isPresent()) {
        return false;
      }
      try (LockResource lr = lockOpt.get()) {
        if (entry.mValue == null) {
          mBackingStore.remove(entry.mKey);
        } else {
          mBackingStore.writeInode(entry.mValue);
        }
        entry.mDirty = false;
      }
      return true;
    }
  }

  private class EdgeCache extends Cache<Edge, Long> {
    private ConcurrentSkipListMap<String, Long> EMPTY = new ConcurrentSkipListMap<>();
    Map<Long, ConcurrentSkipListMap<String, Long>> mIdToChildMap = new ConcurrentHashMap<>();

    public EdgeCache(int maxSize, int highWaterMark, int lowWaterMark) {
      super(maxSize, highWaterMark, lowWaterMark);
    }

    public ConcurrentSkipListMap<String, Long> getChildIds(Long inodeId) {
      return mIdToChildMap.getOrDefault(inodeId, EMPTY);
    }

    @Override
    protected Optional<Long> load(Edge edge) {
      return mBackingStore.getChildId(edge.getId(), edge.getName());
    }

    @Override
    protected boolean flush(Entry entry) {
      Optional<LockResource> lockOpt = mLockManager.tryLockEdge(entry.mKey, LockMode.WRITE);
      if (!lockOpt.isPresent()) {
        return false;
      }
      try (LockResource lr = lockOpt.get()) {
        if (entry.mValue == null) {
          mBackingStore.removeChild(entry.mKey.getId(), entry.mKey.getName());
        } else {
          mBackingStore.addChild(entry.mKey.getId(), entry.mKey.getName(), entry.mValue);
        }
        entry.mDirty = false;
      }
      return true;
    }

    @Override
    protected void onAdd(Edge edge, Long childId) {
      mListingCache.compute(edge.getId(), (key, value) -> {
        System.out.printf("onAdd %s->%s(%s)%n", edge.getId(), edge.getName(), childId);
        if (value != null) {
          synchronized (value) {
            value.mModified = true;
            if (value.mChildren != null) {
              System.out.printf("Put %s->%s%n", edge.getId(), edge.getName());
              value.mChildren.put(edge.getName(), childId);
            }
          }
        }
        return value;
      });
      mIdToChildMap.compute(edge.getId(), (key, value) -> {
        if (value == null) {
          value = new ConcurrentSkipListMap<>();
        }
        value.put(edge.getName(), childId);
        return value;
      });
    }

    @Override
    protected void onRemove(Edge edge, Long childId) {
      mListingCache.compute(edge.getId(), (key, value) -> {
        if (value != null) {
          synchronized (value) {
            value.mModified = true;
            if (value.mChildren != null) {
              value.mChildren.remove(edge.getName());
            }
          }
        }
        return value;
      });
      mIdToChildMap.compute(edge.getId(), (key, value) -> {
        if (value == null) {
          return null;
        }
        value.remove(edge.getName());
        if (value.isEmpty()) {
          return null;
        }
        return value;
      });
    }
  }

  private class ListingCache {
    private Map<Long, ListingCacheEntry> mMap = new ConcurrentHashMap<>();

    public void compute(Long id,
        BiFunction<Long, ListingCacheEntry, ListingCacheEntry> remappingFunction) {
      mMap.compute(id, remappingFunction);
    }

    public Optional<Collection<Long>> getCachedChildIds(Long inodeId) {
      ListingCacheEntry entry = mMap.get(inodeId);
      if (entry != null && entry.mChildren != null) {
        return Optional.of(entry.mChildren.values());
      }
      return Optional.empty();
    }

    public Collection<Long> getChildIds(Long inodeId) {
      ListingCacheEntry entry = mMap.computeIfAbsent(inodeId, x -> new ListingCacheEntry());
      if (entry.mChildren != null) {
        System.out.printf("Get cached ids: %s%n", entry.mChildren.values());
        return entry.mChildren.values();
      }
      if (entry.mLoading.compareAndSet(false, true)) {
        synchronized (entry) {
          if (entry.mChildren != null) {
            System.out.printf("Get cached ids: %s%n", entry.mChildren.values());
            return entry.mChildren.values();
          }
          entry.mModified = false;
        }
        SortedMap<String, Long> listing = computeFullListing(inodeId);
        synchronized (entry) {
          if (!entry.mModified) {
            entry.mChildren = new ConcurrentSkipListMap<>(listing);
          }
          System.out.printf("Get loaded ids: %s%n", entry.mChildren.values());
          return listing.values();
        }
      } else {
        System.out.printf("Get freshly computed ids: %s%n", entry.mChildren.values());
        return computeFullListing(inodeId).values();
      }
    }

    private SortedMap<String, Long> computeFullListing(Long inodeId) {
      Iterator<Entry<String, Long>> cachedChildren =
          new ArrayList<>(mEdgeCache.getChildIds(inodeId).entrySet()).iterator();
      Iterator<? extends Inode> flushedChildren = mBackingStore.getChildren(inodeId).iterator();
      ConcurrentSkipListMap<String, Long> result = new ConcurrentSkipListMap<>();
      Entry<String, Long> cached = nextOrNull(cachedChildren);
      Inode flushed = nextOrNull(flushedChildren);
      while (cached != null && flushed != null) {
        int comparison = cached.getKey().compareTo(flushed.getName());
        if (comparison == 0) {
          // Duplicate children.
          flushed = nextOrNull(flushedChildren);
          cached = nextOrNull(cachedChildren);
          result.put(cached.getKey(), cached.getValue());
        } else if (comparison < 0) {
          cached = nextOrNull(cachedChildren);
          result.put(cached.getKey(), cached.getValue());
        } else {
          flushed = nextOrNull(flushedChildren);
          result.put(flushed.getName(), flushed.getId());
        }
      }
      while (cached != null) {
        result.put(cached.getKey(), cached.getValue());
        cached = nextOrNull(cachedChildren);
      }
      while (flushed != null) {
        result.put(flushed.getName(), flushed.getId());
        flushed = nextOrNull(flushedChildren);
      }
      return result;
    }

    private <T> T nextOrNull(Iterator<T> it) {
      return it.hasNext() ? it.next() : null;
    }
  }

  private class ListingCacheEntry {
    private boolean mModified = false;
    private boolean mAccessed = false;
    private AtomicBoolean mLoading = new AtomicBoolean(false);
    private ConcurrentSkipListMap<String, Long> mChildren = null;
  }
}
