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

package alluxio.master.metastore;

import alluxio.Configuration;
import alluxio.PropertyKey;
import alluxio.master.file.meta.Edge;
import alluxio.master.file.meta.Inode;
import alluxio.master.file.meta.InodeDirectoryView;
import alluxio.master.file.meta.InodeView;
import alluxio.master.file.meta.MutableInode;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.util.Iterator;
import java.util.Optional;

/**
 * An inode store which caches inode tree metadata and delegates to another inode store for cache
 * misses.
 */
public final class CachingInodeStore implements InodeStore {
  private final InodeStore mBase;

  // Cache recently-accessed inodes.
  private final Cache<Long, MutableInode<?>> mInodeCache = CacheBuilder.newBuilder()
      .maximumSize(Configuration.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_SIZE)).build();

  // Cache recently-accessed inode tree edges.
  private final Cache<Edge, Long> mEdgeCache = CacheBuilder.newBuilder()
      .maximumSize(Configuration.getInt(PropertyKey.MASTER_METASTORE_INODE_CACHE_SIZE)).build();

  public CachingInodeStore(InodeStore base) {
    mBase = base;
  }

  @Override
  public Optional<MutableInode<?>> getMutable(long id) {
    Optional<MutableInode<?>> inode = Optional.ofNullable(mInodeCache.getIfPresent(id));
    if (inode.isPresent()) {
      return inode;
    }
    inode = mBase.getMutable(id);
    if (inode.isPresent()) {
      mInodeCache.put(id, inode.get());
    }
    return inode;
  }

  @Override
  public void remove(InodeView inode) {
    mInodeCache.invalidate(inode.getId());
    mEdgeCache.invalidate(new Edge(inode.getParentId(), inode.getName()));
    mBase.remove(inode);
  }

  @Override
  public void writeInode(MutableInode<?> inode) {
    mInodeCache.put(inode.getId(), inode);
    mBase.writeInode(inode);
  }

  @Override
  public void clear() {
    mInodeCache.invalidateAll();
    mEdgeCache.invalidateAll();
    mBase.clear();
  }

  @Override
  public void addChild(long parentId, InodeView inode) {
    mEdgeCache.put(new Edge(parentId, inode.getName()), inode.getId());
    mBase.addChild(parentId, inode);
  }

  @Override
  public void removeChild(long parentId, String name) {
    mEdgeCache.invalidate(new Edge(parentId, name));
    mBase.removeChild(parentId, name);
  }

  @Override
  public long estimateSize() {
    return mBase.estimateSize();
  }

  @Override
  public Iterable<Long> getChildIds(InodeDirectoryView inode) {
    return mBase.getChildIds(inode);
  }

  @Override
  public Iterable<? extends Inode> getChildren(InodeDirectoryView inode) {
    Iterator<Long> baseIterator = mBase.getChildIds(inode).iterator();
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
  public Optional<Long> getChildId(InodeDirectoryView inode, String name) {
    return mBase.getChildId(inode, name);
  }

  @Override
  public Optional<Inode> getChild(InodeDirectoryView inode, String name) {
    Edge edge = new Edge(inode.getId(), name);
    Long childId = mEdgeCache.getIfPresent(edge);
    if (childId != null) {
      return get(childId);
    }
    return mBase.getChildId(inode, name).flatMap(id -> {
      mEdgeCache.put(edge, id);
      return get(id);
    });
  }

  @Override
  public boolean hasChildren(InodeDirectoryView inode) {
    return mBase.hasChildren(inode);
  }
}
