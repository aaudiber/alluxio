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
import alluxio.metrics.MetricsSystem;
import alluxio.resource.LockResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

/**
 * An inode store which caches inode tree metadata and delegates to another inode store for cache
 * misses.
 */
public final class CachingInodeStore implements InodeStore {
  private static final Logger LOG = LoggerFactory.getLogger(CachingInodeStore.class);

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
    mListingCache = new ListingCache(maxSize, highWaterMark, lowWaterMark);
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
  public void writeInode(MutableInode<?> inode, boolean newInodeHint) {
    if (newInodeHint && inode.isDirectory()) {
      mListingCache.addEmptyDirectory(inode.getId());
    }
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
    mEdgeCache.put(new Edge(parentId, childName), childId);
  }

  @Override
  public void removeChild(long parentId, String name) {
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
      return !cached.get().isEmpty();
    }
    return !mEdgeCache.getChildIds(inode.getId()).isEmpty() || mBackingStore.hasChildren(inode);
  }

  private class InodeCache extends Cache<Long, MutableInode<?>> {
    public InodeCache(int maxSize, int highWaterMark, int lowWaterMark) {
      super(maxSize, highWaterMark, lowWaterMark, "inode-cache");
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
    private final ConcurrentSkipListMap<String, Long> mEmpty = new ConcurrentSkipListMap<>();

    private Map<Long, ConcurrentSkipListMap<String, Long>> mIdToChildMap = new ConcurrentHashMap<>();
    private Map<Long, Set<String>> mUnflushedDeletes = new ConcurrentHashMap<>();

    public EdgeCache(int maxSize, int highWaterMark, int lowWaterMark) {
      super(maxSize, highWaterMark, lowWaterMark, "edge-cache");
    }

    public ConcurrentSkipListMap<String, Long> getChildIds(Long inodeId) {
      Iterator<Map.Entry<String, Long>> cachedChildren =
          new ArrayList<>(mIdToChildMap.getOrDefault(inodeId, mEmpty).entrySet()).iterator();
      Set<String> unflushedDeletes =
          new HashSet<>(mUnflushedDeletes.getOrDefault(inodeId, Collections.EMPTY_SET));
      Iterator<? extends Inode> flushedChildren = mBackingStore.getChildren(inodeId).iterator();
      ConcurrentSkipListMap<String, Long> result = new ConcurrentSkipListMap<>();

      Map.Entry<String, Long> cached = nextCachedInode(cachedChildren);
      Inode flushed = nextFlushedInode(flushedChildren, unflushedDeletes);
      while (cached != null && flushed != null) {
        int comparison = cached.getKey().compareTo(flushed.getName());
        if (comparison == 0) {
          // De-duplicate children with the same name.
          result.put(cached.getKey(), cached.getValue());
          flushed = nextFlushedInode(flushedChildren, unflushedDeletes);
          cached = nextCachedInode(cachedChildren);
        } else if (comparison < 0) {
          result.put(cached.getKey(), cached.getValue());
          cached = nextCachedInode(cachedChildren);
        } else {
          result.put(flushed.getName(), flushed.getId());
          flushed = nextFlushedInode(flushedChildren, unflushedDeletes);
        }
      }
      while (cached != null) {
        result.put(cached.getKey(), cached.getValue());
        cached = nextCachedInode(cachedChildren);
      }
      while (flushed != null) {
        result.put(flushed.getName(), flushed.getId());
        flushed = nextFlushedInode(flushedChildren, unflushedDeletes);
      }
      return result;
    }

    private Inode nextFlushedInode(Iterator<? extends Inode> flushedIterator,
        Set<String> unflushedDeletes) {
      while (flushedIterator.hasNext()) {
        Inode inode = flushedIterator.next();
        if (!unflushedDeletes.contains(inode.getName())) {
          return inode;
        }
      }
      return null;
    }

    private <T> T nextCachedInode(Iterator<T> cacheIterator) {
      return cacheIterator.hasNext() ? cacheIterator.next() : null;
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
        if (value != null) {
          synchronized (value) {
            value.mModified = true;
            if (value.mChildren != null) {
              value.addChild(edge.getName(), childId);
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
    protected void onRemove(Edge edge) {
      mListingCache.compute(edge.getId(), (key, value) -> {
        if (value != null) {
          synchronized (value) {
            value.mModified = true;
            if (value.mChildren != null) {
              value.removeChild(edge.getName());
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
      mUnflushedDeletes.compute(edge.getId(), (key, value) -> {
        if (value == null) {
          value = new HashSet<>();
        }
        value.add(edge.getName());
        return value;
      });
    }

    @Override
    protected void onEvict(Edge edge, Long childId) {
      mUnflushedDeletes.computeIfPresent(edge.getId(), (key, value) -> {
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
    private final int mMaxSize;
    private final int mHighWaterMark;
    private final int mLowWaterMark;
    private AtomicLong mWeight = new AtomicLong(0);

    private Map<Long, ListingCacheEntry> mMap = new ConcurrentHashMap<>();
    private Iterator<Map.Entry<Long, ListingCacheEntry>> mEvictionHead = mMap.entrySet().iterator();

    private ListingCache(int maxSize, int highWaterMark, int lowWaterMark) {
      mMaxSize = maxSize;
      mHighWaterMark = highWaterMark;
      mLowWaterMark = lowWaterMark;
      MetricsSystem.registerGaugeIfAbsent(MetricsSystem.getMetricName("listing-cache-size"),
          () -> mWeight.get());
    }

    public void compute(Long id,
        BiFunction<Long, ListingCacheEntry, ListingCacheEntry> remappingFunction) {
      mMap.compute(id, remappingFunction);
    }

    public Optional<Collection<Long>> getCachedChildIds(Long inodeId) {
      ListingCacheEntry entry = mMap.get(inodeId);
      if (entry != null && entry.mChildren != null) {
        entry.mAccessed = true;
        return Optional.of(entry.mChildren.values());
      }
      return Optional.empty();
    }

    public Collection<Long> getChildIds(Long inodeId) {
      ListingCacheEntry entry = mMap.compute(inodeId, (key, value) -> {
        if (value == null) {
          return new ListingCacheEntry();
        }
        value.mAccessed = true;
        return value;
      });
      if (entry.mChildren != null) {
        return entry.mChildren.values();
      }
      if (mWeight.get() < mMaxSize && entry.mLoading.compareAndSet(false, true)) {
        try {
          return load(inodeId, entry).values();
        } finally {
          entry.mLoading.set(false);
        }
      }
      return mEdgeCache.getChildIds(inodeId).values();
    }

    public void addEmptyDirectory(long id) {
      mMap.computeIfAbsent(id, x -> {
        ListingCacheEntry entry = new ListingCacheEntry();
        entry.mChildren = new ConcurrentSkipListMap<>();
        return entry;
      });
    }

    private SortedMap<String, Long> load(Long inodeId, ListingCacheEntry entry) {
      synchronized (entry) {
        if (entry.mChildren != null) {
          return entry.mChildren;
        }
        entry.mModified = false;
      }
      SortedMap<String, Long> listing = mEdgeCache.getChildIds(inodeId);
      if (mWeight.get() > mHighWaterMark) {
        evict();
      }
      synchronized (entry) {
        if (!entry.mModified) {
          entry.mChildren = new ConcurrentSkipListMap<>(listing);
          mWeight.addAndGet(entry.mChildren.size() + 1);
        }
      }
      return listing;
    }

    private void evict() {
      long startTime = System.currentTimeMillis();
      long toEvict = mMap.size() - mLowWaterMark;
      while (toEvict > 0) {
        if (!mEvictionHead.hasNext()) {
          mEvictionHead = mMap.entrySet().iterator();
        }
        if (!mEvictionHead.hasNext()) {
          break; // cache is empty.
        }
        Entry<Long, ListingCacheEntry> candidate = mEvictionHead.next();
        if (candidate.getValue().mAccessed) {
          candidate.getValue().mAccessed = false;
        }
        mMap.compute(candidate.getKey(), (key, value) -> {
          if (value != null && value.mChildren != null) {
            mWeight.addAndGet(-(value.mChildren.size() + 1));
            return null;
          }
          return value;
        });
      }
      LOG.info("Evicted weight={} from listing cache down to weight={} in {}ms", toEvict,
          mMap.size(), System.currentTimeMillis() - startTime);
    }

    private class ListingCacheEntry {
      private boolean mModified = false;
      private boolean mAccessed = false;
      private AtomicBoolean mLoading = new AtomicBoolean(false);
      private ConcurrentSkipListMap<String, Long> mChildren = null;

      public void addChild(String name, Long id) {
        if (mChildren.put(name, id) == null) {
          mWeight.incrementAndGet();
        }
      }

      public void removeChild(String name) {
        if (mChildren.remove(name) != null) {
          mWeight.decrementAndGet();
        }
      }
    }
  }
}