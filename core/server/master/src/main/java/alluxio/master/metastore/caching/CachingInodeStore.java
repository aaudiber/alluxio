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

import java.util.Iterator;
import java.util.Optional;

/**
 * An inode store which caches inode tree metadata and delegates to another inode store for cache
 * misses.
 */
public final class CachingInodeStore implements InodeStore {
  private final InodeStore mBackingStore;
  private final InodeLockManager mLockManager;

  // Cache recently-accessed inodes.
  private final Cache<Long, MutableInode<?>> mInodeCache;

  // Cache recently-accessed inode tree edges.
  private final Cache<Edge, Long> mEdgeCache;

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
    return mBackingStore.getChildIds(inodeId);
  }

  @Override
  public Iterable<? extends Inode> getChildren(Long inodeId) {
    Iterator<Long> baseIterator = mBackingStore.getChildIds(inodeId).iterator();
    return () -> new Iterator<Inode>() {
      @Override
      public boolean hasNext() {
        return baseIterator.hasNext();
      }

      @Override
      public Inode next() {
        return get(baseIterator.next()).get();
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
    return mBackingStore.hasChildren(inode);
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
    protected Optional<LockResource> tryLock(Long inodeId) {
      return mLockManager.tryLockInode(inodeId, LockMode.WRITE);
    }

    @Override
    protected void evictToBackingStore(Long id, MutableInode<?> inode) {
      mBackingStore.writeInode(inode);
    }

    @Override
    protected void removeFromBackingStore(Long inodeId) {
      mBackingStore.remove(inodeId);
    }
  }

  private class EdgeCache extends Cache<Edge, Long> {
    public EdgeCache(int maxSize, int highWaterMark, int lowWaterMark) {
      super(maxSize, highWaterMark, lowWaterMark);
    }

    @Override
    protected Optional<Long> load(Edge edge) {
      return mBackingStore.getChildId(edge.getId(), edge.getName());
    }

    @Override
    protected Optional<LockResource> tryLock(Edge edge) {
      return mLockManager.tryLockEdge(edge, LockMode.WRITE);
    }

    @Override
    protected void evictToBackingStore(Edge edge, Long id) {
      mBackingStore.addChild(edge.getId(), edge.getName(), id);
    }

    @Override
    protected void removeFromBackingStore(Edge edge) {
      mBackingStore.removeChild(edge.getId(), edge.getName());
    }
  }
}
